package transactionsx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

func (t *TransactionAttempt) Insert(ctx context.Context, opts InsertOptions) (*GetResult, error) {
	result, errSt := t.insert(ctx, opts)
	if errSt != nil {
		t.logger.Info("insert failed", zap.Error(errSt.Err()))
		return nil, errSt.Err()
	}

	return result, nil
}

func (t *TransactionAttempt) insert(
	ctx context.Context,
	opts InsertOptions,
) (*GetResult, *transactionOperationStatus) {
	t.logger.Info("performing insert",
		zaputils.FQDocID("key", opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName, opts.Key))

	t.lock.Lock()
	t.beginOpLocked()

	err := t.checkCanPerformOpLocked()
	if err != nil {
		t.lock.Unlock()
		t.endOp()
		return nil, err
	}

	agent := opts.Agent
	oboUser := opts.OboUser
	scopeName := opts.ScopeName
	collectionName := opts.CollectionName
	key := opts.Key
	value := opts.Value

	expired := t.checkExpiredAtomic(ctx, hookStageInsert, key, false)
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
		case StagedMutationRemove:
			t.logger.Info("staged remove exists on doc, performing replace")
			result, err := t.stageReplace(
				ctx, agent, oboUser, scopeName, collectionName, key, value, existingMutation.Cas)
			t.endOp()
			return result, err
		case StagedMutationInsert:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrDocExists, "attempted to insert a document previously inserted in this transaction")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case StagedMutationReplace:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrDocExists, "attempted to insert a document previously replaced in this transaction")),
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

	err = t.confirmATRPending(ctx, agent, oboUser, key)
	if err != nil {
		t.endOp()
		return nil, err
	}

	result, aerr := t.stageInsert(
		ctx, agent, oboUser, scopeName, collectionName, key, value, 0)
	t.endOp()
	return result, aerr

}

func (t *TransactionAttempt) resolveConflictedInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
) (*GetResult, *transactionOperationStatus) {
	getRes, err := t.getMetaForConflictedInsert(ctx, agent, oboUser, scopeName, collectionName, key)
	if err != nil {
		return nil, err
	}

	isTombstone := getRes.IsTombstone
	txnMeta := getRes.Xattr
	cas := getRes.Cas

	if txnMeta == nil {
		// This doc isn't in a transaction
		if !isTombstone {
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocExists, "found non-tombstone doc during conflicted insert resolution"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		}

		// There wasn't actually a staged mutation there.
		return t.stageInsert(ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
	}

	meta := &MutableItemMeta{
		TransactionID: txnMeta.ID.Transaction,
		AttemptID:     txnMeta.ID.Attempt,
		ATR: MutableItemMetaATR{
			BucketName:     txnMeta.ATR.BucketName,
			ScopeName:      txnMeta.ATR.ScopeName,
			CollectionName: txnMeta.ATR.CollectionName,
			DocID:          txnMeta.ATR.DocID,
		},
		ForwardCompat: forwardCompatFromJson(txnMeta.ForwardCompat),
	}

	err = t.checkForwardCompatability(
		ctx,
		key,
		agent.BucketName(),
		scopeName,
		collectionName,
		forwardCompatStageWWCInsertingGet, meta.ForwardCompat, false)
	if err != nil {
		return nil, err
	}

	if txnMeta.Operation.Type != MutationTypeJsonInsert {
		return nil, t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrDocExists, "found staged non-insert mutation")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	// We have guards in place within the write write conflict polling to prevent miss-use when
	// an existing mutation must have been discovered before it's safe to overwrite.  This logic
	// is unneccessary, as is the forwards compatibility check when resolving conflicted inserts
	// so we can safely just ignore it.
	if meta.TransactionID == t.transactionID && meta.AttemptID == t.id {
		return t.stageInsert(ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
	}

	err = t.writeWriteConflictPoll(
		ctx, forwardCompatStageWWCInserting, agent.BucketName(), scopeName, collectionName, key, cas, meta, nil)
	if err != nil {
		return nil, err
	}

	cas, err = t.cleanupStagedInsert(ctx, agent, oboUser, scopeName, collectionName, key, cas, isTombstone)
	if err != nil {
		return nil, err
	}

	return t.stageInsert(ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
}

func (t *TransactionAttempt) stageInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	value json.RawMessage,
	cas uint64,
) (*GetResult, *transactionOperationStatus) {
	expired := t.checkExpiredAtomic(ctx, hookStageInsert, key, false)
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

	stagedInfo, err := invokeHookWithDocID(ctx, t.hooks.StagedInsert, key, func() (*stagedMutation, error) {
		stagedInfo := &stagedMutation{
			OpType:         StagedMutationInsert,
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
		txnMeta.Operation.Type = MutationTypeJsonInsert
		txnMeta.Operation.Staged = stagedInfo.Staged

		txnMetaBytes, err := json.Marshal(txnMeta)
		if err != nil {
			return nil, err
		}

		flags := memdx.SubdocDocFlagCreateAsDeleted | memdx.SubdocDocFlagAccessDeleted
		var txnOp memdx.MutateInOpType
		if cas == 0 {
			flags |= memdx.SubdocDocFlagAddDoc
			txnOp = memdx.MutateInOpTypeDictAdd
		} else {
			txnOp = memdx.MutateInOpTypeDictSet
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
					Op:    txnOp,
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
			},
			DurabilityLevel: memdDuraLevel,
			Flags:           flags,
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
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.stageInsert(ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
			case <-ctx.Done():
				return nil, t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailTransient:
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
		case TransactionErrorClassFailDocAlreadyExists:
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			return t.resolveConflictedInsert(ctx, agent, oboUser, scopeName, collectionName, key, value)
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

type conflictedInsertMeta struct {
	IsTombstone bool
	Xattr       *TxnXattrJson
	Cas         uint64
}

func (t *TransactionAttempt) getMetaForConflictedInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
) (*conflictedInsertMeta, *transactionOperationStatus) {
	meta, err := invokeHookWithDocID(ctx, t.hooks.GetDocInExistsDuringStagedInsert, key, func() (*conflictedInsertMeta, error) {
		result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Ops: []memdx.LookupInOp{
				{
					Op:    memdx.LookupInOpTypeGet,
					Path:  []byte("txn"),
					Flags: memdx.SubdocOpFlagXattrPath,
				},
			},
			Flags:      memdx.SubdocDocFlagAccessDeleted,
			OnBehalfOf: oboUser,
		})
		if err != nil {
			return nil, err
		}

		var txnMeta *TxnXattrJson
		if result.Ops[0].Err == nil {
			var txnMetaVal TxnXattrJson
			if err := json.Unmarshal(result.Ops[0].Value, &txnMetaVal); err != nil {
				return nil, err
			}
			txnMeta = &txnMetaVal
		}

		return &conflictedInsertMeta{
			IsTombstone: result.DocIsDeleted,
			Xattr:       txnMeta,
			Cas:         result.Cas,
		}, nil
	})
	if err != nil {
		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailPathNotFound:
			fallthrough
		case TransactionErrorClassFailTransient:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
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

	return meta, nil
}

func (t *TransactionAttempt) cleanupStagedInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	cas uint64,
	isTombstone bool,
) (uint64, *transactionOperationStatus) {
	if isTombstone {
		// This is already a tombstone, so we can just proceed.
		return cas, nil
	}

	cas, err := invokeHookWithDocID(ctx, t.hooks.RemovingDocDuringStagedInsert, key, func() (uint64, error) {
		result, err := agent.Delete(ctx, &gocbcorex.DeleteOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			OnBehalfOf:     oboUser,
		})
		if err != nil {
			return 0, err
		}

		return result.Cas, nil
	})
	if err != nil {
		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			fallthrough
		case TransactionErrorClassFailTransient:
			return 0, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			return 0, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	return cas, nil
}
