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

func (t *TransactionAttempt) Insert(ctx context.Context, opts TransactionInsertOptions) (*TransactionGetResult, error) {
	result, errSt := t.insert(ctx, opts)
	if errSt != nil {
		t.logger.Info("insert failed", zap.Error(errSt.Err()))
		return nil, t.processOpStatus(ctx, errSt)
	}

	return result, nil
}

func (t *TransactionAttempt) insert(
	ctx context.Context,
	opts TransactionInsertOptions,
) (*TransactionGetResult, *TransactionOperationStatus) {
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

	cerr := t.checkExpiredAtomic(ctx, hookInsert, key, false)
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
		case TransactionStagedMutationRemove:
			t.logger.Info("staged remove exists on doc, performing replace")
			result, err := t.stageReplace(
				ctx, agent, oboUser, scopeName, collectionName, key, value, existingMutation.Cas)
			t.endOp()
			return result, err
		case TransactionStagedMutationInsert:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					// TODO(brett19): Correct error?
					wrapError(ErrDocExists, "attempted to insert a document previously inserted in this transaction")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionStagedMutationReplace:
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					// TODO(brett19): Correct error?
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
) (*TransactionGetResult, *TransactionOperationStatus) {
	isTombstone, txnMeta, cas, err := t.getMetaForConflictedInsert(ctx, agent, oboUser, scopeName, collectionName, key)
	if err != nil {
		return nil, err
	}

	if txnMeta == nil {
		// This doc isn't in a transaction
		if !isTombstone {
			// TODO(brett19): Consider how this works
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

	meta := &TransactionMutableItemMeta{
		TransactionID: txnMeta.ID.Transaction,
		AttemptID:     txnMeta.ID.Attempt,
		ATR: TransactionMutableItemMetaATR{
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

	if txnMeta.Operation.Type != jsonMutationInsert {
		// TODO(brett19): correct error?
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
) (*TransactionGetResult, *TransactionOperationStatus) {
	ecCb := func(result *TransactionGetResult, cerr *classifiedError) (*TransactionGetResult, *TransactionOperationStatus) {
		if cerr == nil {
			return result, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.stageInsert(ctx, agent, oboUser, scopeName, collectionName, key, value, cas)
			case <-ctx.Done():
				return nil, t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
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

	cerr := t.checkExpiredAtomic(ctx, hookInsert, key, false)
	if cerr != nil {
		return ecCb(nil, cerr)
	}

	err := t.hooks.BeforeStagedInsert(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

	stagedInfo := &transactionStagedMutation{
		OpType:         TransactionStagedMutationInsert,
		Agent:          agent,
		OboUser:        oboUser,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            key,
		Staged:         value,
	}

	var txnMeta jsonTxnXattr
	txnMeta.ID.Transaction = t.transactionID
	txnMeta.ID.Attempt = t.id
	txnMeta.ATR.CollectionName = t.atrCollectionName
	txnMeta.ATR.ScopeName = t.atrScopeName
	txnMeta.ATR.BucketName = t.atrAgent.BucketName()
	txnMeta.ATR.DocID = string(t.atrKey)
	txnMeta.Operation.Type = jsonMutationInsert
	txnMeta.Operation.Staged = stagedInfo.Staged

	txnMetaBytes, err := json.Marshal(txnMeta)
	if err != nil {
		return ecCb(nil, classifyError(err))
	}

	flags := memdx.SubdocDocFlagCreateAsDeleted | memdx.SubdocDocFlagAccessDeleted
	var txnOp memdx.MutateInOpType
	if cas == 0 {
		flags |= memdx.SubdocDocFlagAddDoc
		txnOp = memdx.MutateInOpTypeDictAdd
	} else {
		txnOp = memdx.MutateInOpTypeDictSet
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
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		Flags:           flags,
		OnBehalfOf:      stagedInfo.OboUser,
	})
	if err != nil {
		return ecCb(nil, classifyError(err))
	}

	stagedInfo.Cas = result.Cas

	err = t.hooks.AfterStagedInsertComplete(ctx, key)
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

func (t *TransactionAttempt) getMetaForConflictedInsert(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
) (bool, *jsonTxnXattr, uint64, *TransactionOperationStatus) {
	ecCb := func(isTombstone bool, meta *jsonTxnXattr, cas uint64, cerr *classifiedError) (bool, *jsonTxnXattr, uint64, *TransactionOperationStatus) {
		if cerr == nil {
			return isTombstone, meta, cas, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailPathNotFound:
			fallthrough
		case TransactionErrorClassFailTransient:
			return isTombstone, nil, 0, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			return isTombstone, nil, 0, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	err := t.hooks.BeforeGetDocInExistsDuringStagedInsert(ctx, key)
	if err != nil {
		return ecCb(false, nil, 0, classifyHookError(err))
	}

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
		return ecCb(false, nil, 0, classifyError(err))
	}

	var txnMeta *jsonTxnXattr
	if result.Ops[0].Err == nil {
		var txnMetaVal jsonTxnXattr
		if err := json.Unmarshal(result.Ops[0].Value, &txnMetaVal); err != nil {
			return ecCb(false, nil, 0, classifyError(err))
		}
		txnMeta = &txnMetaVal
	}

	isTombstone := result.DocIsDeleted
	return isTombstone, txnMeta, result.Cas, nil
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
) (uint64, *TransactionOperationStatus) {
	ecCb := func(cas uint64, cerr *classifiedError) (uint64, *TransactionOperationStatus) {
		if cerr == nil {
			return cas, nil
		}

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

	if isTombstone {
		// This is already a tombstone, so we can just proceed.
		return cas, nil
	}

	err := t.hooks.BeforeRemovingDocDuringStagedInsert(ctx, key)
	if err != nil {
		return ecCb(0, classifyHookError(err))
	}

	result, err := agent.Delete(ctx, &gocbcorex.DeleteOptions{
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            key,
		OnBehalfOf:     oboUser,
	})
	if err != nil {
		return ecCb(0, classifyError(err))
	}

	return result.Cas, nil
}
