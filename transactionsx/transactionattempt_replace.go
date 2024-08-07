package transactionsx

import (
	"context"
	"encoding/json"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

func (t *TransactionAttempt) Replace(ctx context.Context, opts ReplaceOptions) (*GetResult, error) {
	result, errSt := t.replace(ctx, opts)
	if errSt != nil {
		t.logger.Info("replace failed", zap.Error(errSt.Err()))
		return nil, errSt.Err()
	}

	return result, nil
}

func (t *TransactionAttempt) replace(
	ctx context.Context,
	opts ReplaceOptions,
) (*GetResult, *transactionOperationStatus) {
	t.logger.Info("performing replace",
		zaputils.FQDocID("key", opts.Document.agent.BucketName(), opts.Document.scopeName, opts.Document.collectionName, opts.Document.key))

	t.lock.Lock()
	t.beginOpLocked()

	err := t.checkCanPerformOpLocked()
	if err != nil {
		t.lock.Unlock()
		t.endOp()
		return nil, err
	}

	agent := opts.Document.agent
	oboUser := opts.Document.oboUser
	scopeName := opts.Document.scopeName
	collectionName := opts.Document.collectionName
	key := opts.Document.key
	value := opts.Value
	cas := opts.Document.Cas
	meta := opts.Document.Meta

	expired := t.checkExpiredAtomic(ctx, hookStageReplace, key, false)
	if expired {
		t.lock.Unlock()
		t.endOp()
		return nil, t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), scopeName, collectionName, key)
	t.lock.Unlock()

	if existingMutation != nil {
		switch existingMutation.OpType {
		case StagedMutationInsert:
			t.logger.Info("staged insert exists on doc, performing insert")

			result, err := t.stageInsert(
				ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
			t.endOp()
			return result, err

		case StagedMutationReplace:
			t.logger.Info("staged replace exists on doc, this is ok")

			// We can overwrite other replaces without issue, any conflicts between the mutation
			// the user passed to us and the existing mutation is caught by WriteWriteConflict.
		case StagedMutationRemove:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrDocNotFound, "attempted to replace a document previously removed in this transaction")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrIllegalState, "unexpected staged mutation type")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	err = t.writeWriteConflictPoll(
		ctx,
		forwardCompatStageWWCReplacing,
		agent.BucketName(), scopeName, collectionName, key, cas,
		meta,
		existingMutation)
	if err != nil {
		t.endOp()
		return nil, err
	}

	err = t.confirmATRPending(ctx, agent, oboUser, key)
	if err != nil {
		t.endOp()
		return nil, err
	}

	result, aerr := t.stageReplace(
		ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
	t.endOp()
	return result, aerr
}

func (t *TransactionAttempt) stageReplace(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
	cas uint64,
) (*GetResult, *transactionOperationStatus) {
	expired := t.checkExpiredAtomic(ctx, hookStageRemove, key, false)
	if expired {
		t.setExpiryOvertimeAtomic()
		return nil, t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	stagedInfo, err := invokeHookWithDocID(ctx, t.hooks.StagedReplace, key, func() (*stagedMutation, error) {
		stagedInfo := &stagedMutation{
			OpType:         StagedMutationReplace,
			Agent:          agent,
			OboUser:        oboUser,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Staged:         value,
		}

		var txnMeta TxnXattrJson
		txnMeta.ID.Transaction = t.transactionID
		txnMeta.ID.Attempt = t.id
		txnMeta.ATR.CollectionName = t.atrCollectionName
		txnMeta.ATR.ScopeName = t.atrScopeName
		txnMeta.ATR.BucketName = t.atrAgent.BucketName()
		txnMeta.ATR.DocID = string(t.atrKey)
		txnMeta.Operation.Type = MutationTypeJsonReplace
		txnMeta.Operation.Staged = stagedInfo.Staged
		txnMeta.Restore = &TxnXattrRestoreJson{
			OriginalCAS: "",
			ExpiryTime:  0,
			RevID:       "",
		}

		txnMetaBytes, err := json.Marshal(txnMeta)
		if err != nil {
			return nil, err
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return nil, err
		}

		result, err := stagedInfo.Agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
			ScopeName:      stagedInfo.ScopeName,
			CollectionName: stagedInfo.CollectionName,
			Key:            stagedInfo.Key,
			Cas:            cas,
			Ops: []memdx.MutateInOp{
				{
					Op:    memdx.MutateInOpTypeDictSet,
					Path:  []byte("txn"),
					Flags: memdx.SubdocOpFlagMkDirP | memdx.SubdocOpFlagXattrPath,
					Value: txnMetaBytes,
				},
				{
					Op:    memdx.MutateInOpTypeDictSet,
					Path:  []byte("txn.op.crc32"),
					Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					Value: memdx.SubdocMacroNewCrc32c,
				},
				{
					Op:    memdx.MutateInOpTypeDictSet,
					Path:  []byte("txn.restore.CAS"),
					Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					Value: memdx.SubdocMacroOldCas,
				},
				{
					Op:    memdx.MutateInOpTypeDictSet,
					Path:  []byte("txn.restore.exptime"),
					Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					Value: memdx.SubdocMacroOldExptime,
				},
				{
					Op:    memdx.MutateInOpTypeDictSet,
					Path:  []byte("txn.restore.revid"),
					Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					Value: memdx.SubdocMacroOldRevID,
				},
			},
			Flags:           memdx.SubdocDocFlagAccessDeleted,
			DurabilityLevel: memdDuraLevel,
			OnBehalfOf:      stagedInfo.OboUser,
		})
		if err != nil {
			return nil, err
		}

		stagedInfo.Cas = result.Cas

		return stagedInfo, nil
	})
	if err != nil {
		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrDocNotFound, "document not found during staging")),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailDocAlreadyExists:
			cerr.Class = TransactionErrorClassFailCasMismatch
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailTransient:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailAmbiguous:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailHard:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	t.recordStagedMutation(stagedInfo)

	return &GetResult{
		agent:          stagedInfo.Agent,
		oboUser:        stagedInfo.OboUser,
		scopeName:      stagedInfo.ScopeName,
		collectionName: stagedInfo.CollectionName,
		key:            stagedInfo.Key,
		Value:          stagedInfo.Staged,
		Cas:            stagedInfo.Cas,
		Meta:           nil,
	}, nil
}
