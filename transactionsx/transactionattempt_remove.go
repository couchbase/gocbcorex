package transactionsx

import (
	"context"
	"encoding/json"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
)

func (t *transactionAttempt) Remove(ctx context.Context, opts TransactionRemoveOptions) (*TransactionGetResult, error) {
	result, err := t.remove(ctx, opts)
	if err != nil {
		t.logger.Info("Remove failed")

		if err.shouldNotRollback {
			t.ensureCleanUpRequest()
		}

		return nil, err
	}

	return result, nil
}

func (t *transactionAttempt) remove(
	ctx context.Context,
	opts TransactionRemoveOptions,
) (*TransactionGetResult, *TransactionOperationFailedError) {
	t.logger.Info("Performing remove",
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
	cas := opts.Document.Cas
	meta := opts.Document.Meta

	cerr := t.checkExpiredAtomic(ctx, hookRemove, key, false)
	if cerr != nil {
		t.lock.Unlock()
		t.endOp()
		return nil, t.operationFailed(operationFailedDef{
			Cerr:              cerr,
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	_, existingMutation := t.getStagedMutationLocked(agent.BucketName(), scopeName, collectionName, key)
	t.lock.Unlock()

	if existingMutation != nil {
		switch existingMutation.OpType {
		case TransactionStagedMutationInsert:
			t.logger.Info("Staged insert exists on doc, removing txn metadata")

			result, err := t.stageRemoveOfInsert(
				ctx, agent, oboUser, scopeName, collectionName, key, cas)
			t.endOp()
			return result, err
		case TransactionStagedMutationReplace:
			t.logger.Info("Staged replace exists on doc, this is ok")

			// We can overwrite other replaces without issue, any conflicts between the mutation
			// the user passed to us and the existing mutation is caught by WriteWriteConflict.
		case TransactionStagedMutationRemove:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					// TODO(brett19): Right error?
					wrapError(memdx.ErrDocNotFound, "attempted to remove a document previously removed in this transaction")),
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
		forwardCompatStageWWCRemoving,
		agent, oboUser, scopeName, collectionName, key, cas,
		meta,
		existingMutation)
	if err != nil {
		t.endOp()
		return nil, err
	}

	err = t.confirmATRPending(ctx, agent, oboUser, scopeName, collectionName, key)
	if err != nil {
		t.endOp()
		return nil, err
	}

	result, err := t.stageRemove(
		ctx, agent, oboUser, scopeName, collectionName, key, cas)
	t.endOp()
	return result, err
}

func (t *transactionAttempt) stageRemove(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	cas uint64,
) (*TransactionGetResult, *TransactionOperationFailedError) {
	ecCb := func(result *TransactionGetResult, cerr *classifiedError) (*TransactionGetResult, *TransactionOperationFailedError) {
		if cerr == nil {
			return result, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
		case TransactionErrorClassFailDocNotFound:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					// TODO(brett19): Right error?
					wrapError(memdx.ErrDocNotFound, "document not found during staging")),
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

	cerr := t.checkExpiredAtomic(ctx, hookRemove, key, false)
	if cerr != nil {
		return ecCb(nil, cerr)
	}

	err := t.hooks.BeforeStagedRemove(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

	stagedInfo := &transactionStagedMutation{
		OpType:         TransactionStagedMutationRemove,
		Agent:          agent,
		OboUser:        oboUser,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            key,
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = t.transactionID
	txnMeta.ID.Attempt = t.id
	txnMeta.ATR.CollectionName = t.atrCollectionName
	txnMeta.ATR.ScopeName = t.atrScopeName
	txnMeta.ATR.BucketName = t.atrAgent.BucketName()
	txnMeta.ATR.DocID = string(t.atrKey)
	txnMeta.Operation.Type = jsonMutationRemove
	txnMeta.Restore = &jsonTxnXattrRestore{
		OriginalCAS: "",
		ExpiryTime:  0,
		RevID:       "",
	}

	txnMetaBytes, err := json.Marshal(txnMeta)
	if err != nil {
		return ecCb(nil, classifyError(err))
	}

	flags := memdx.SubdocDocFlagAccessDeleted

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
				Value: memdx.SubdocMacroCrc32c,
			},
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("txn.restore.CAS"),
				Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
				Value: memdx.SubdocMacroCas,
			},
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("txn.restore.exptime"),
				Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
				Value: memdx.SubdocMacroExptime,
			},
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("txn.restore.revid"),
				Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
				Value: memdx.SubdocMacroRevID,
			},
		},
		Flags:           flags,
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		OnBehalfOf:      stagedInfo.OboUser,
	})
	if err != nil {
		return ecCb(nil, classifyError(err))
	}

	stagedInfo.Cas = result.Cas

	err = t.hooks.AfterStagedRemoveComplete(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

	t.recordStagedMutation(stagedInfo)

	return &TransactionGetResult{
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

func (t *transactionAttempt) stageRemoveOfInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	cas uint64,
) (*TransactionGetResult, *TransactionOperationFailedError) {
	ecCb := func(result *TransactionGetResult, cerr *classifiedError) (*TransactionGetResult, *TransactionOperationFailedError) {
		if cerr == nil {
			return result, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailExpiry:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
		case TransactionErrorClassFailDocNotFound:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					// TODO(brett19): Right error?
					wrapError(memdx.ErrDocNotFound, "staged document was modified since insert")),
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

	cerr := t.checkExpiredAtomic(ctx, hookRemoveStagedInsert, key, false)
	if cerr != nil {
		return ecCb(nil, cerr)
	}

	err := t.hooks.BeforeRemoveStagedInsert(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

	result, err := agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            key,
		Cas:            cas,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDelete,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		OnBehalfOf:      oboUser,
	})
	if err != nil {
		return ecCb(nil, classifyError(err))
	}

	err = t.hooks.AfterRemoveStagedInsert(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

	t.removeStagedMutation(agent.BucketName(), scopeName, collectionName, key)

	return &TransactionGetResult{
		agent:          agent,
		oboUser:        oboUser,
		scopeName:      scopeName,
		collectionName: collectionName,
		key:            key,
		Cas:            result.Cas,
	}, nil
}
