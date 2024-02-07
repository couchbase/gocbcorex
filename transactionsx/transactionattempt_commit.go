package transactionsx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func (t *transactionAttempt) Commit(ctx context.Context) error {
	t.logger.Info("Committing")

	err := t.commit(ctx)
	if err != nil {
		t.logger.Info("Commit failed", zap.Error(err))

		if t.ShouldRollback() {
			if !t.isExpiryOvertimeAtomic() {
				t.applyStateBits(transactionStateBitPreExpiryAutoRollback, 0)
			}

			rerr := t.rollback(ctx)
			if rerr != nil {
				t.logger.Info("Rollback after commit failure failed",
					zap.Error(rerr), zap.NamedError("commitError", err))
			}

			t.ensureCleanUpRequest()
			return err
		}

		t.ensureCleanUpRequest()
		return err
	}

	t.applyStateBits(transactionStateBitShouldNotRetry|transactionStateBitShouldNotRollback, 0)

	t.ensureCleanUpRequest()
	return nil
}

func (t *transactionAttempt) commit(
	ctx context.Context,
) *TransactionOperationFailedError {
	t.lock.Lock()

	err := t.waitForOpsLocked(ctx)
	if err != nil {
		t.lock.Unlock()
		return err
	}

	err = t.checkCanCommitLocked()
	if err != nil {
		t.lock.Unlock()
		return err
	}

	t.applyStateBits(transactionStateBitShouldNotCommit, 0)

	if t.state == TransactionAttemptStateNothingWritten {
		t.lock.Unlock()
		return nil
	}

	cerr := t.checkExpiredAtomic(ctx, hookCommit, nil, false)
	if cerr != nil {
		t.lock.Unlock()
		return t.operationFailed(operationFailedDef{
			Cerr:              cerr,
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	t.state = TransactionAttemptStateCommitting

	t.lock.Unlock()

	err = t.setATRCommittedExclusive(ctx, false)
	if err != nil {
		var newState TransactionAttemptState
		if err.shouldRaise == TransactionErrorReasonTransactionFailedPostCommit {
			newState = TransactionAttemptStateCommitted
		} else if err.shouldRaise != TransactionErrorReasonTransactionCommitAmbiguous {
			newState = TransactionAttemptStatePending
		}

		t.lock.Lock()
		t.state = newState
		t.lock.Unlock()

		return err
	}

	t.lock.Lock()
	t.state = TransactionAttemptStateCommitted
	t.lock.Unlock()

	var mutErrs []*TransactionOperationFailedError
	if !t.enableParallelUnstaging {
		for _, mutation := range t.stagedMutations {
			err := t.commitStagedMutation(ctx, mutation)
			if err != nil {
				mutErrs = append(mutErrs, err)
				break
			}
		}
	} else {
		numThreads := 32
		numMutations := len(t.stagedMutations)
		pendCh := make(chan *transactionStagedMutation, numThreads*2)
		waitCh := make(chan *TransactionOperationFailedError, numMutations)

		// Start all our threads to process mutations
		for threadIdx := 0; threadIdx < numThreads; threadIdx++ {
			go func(threadIdx int) {
				for {
					mutation, ok := <-pendCh
					if !ok {
						break
					}

					err := t.commitStagedMutation(ctx, mutation)
					waitCh <- err
				}
			}(threadIdx)
		}

		// Send all the mutations
		for _, mutation := range t.stagedMutations {
			pendCh <- mutation
		}
		close(pendCh)

		// Wait for all the responses
		for i := 0; i < numMutations; i++ {
			err := <-waitCh
			if err != nil {
				mutErrs = append(mutErrs, err)
			}
		}
	}
	err = mergeOperationFailedErrors(mutErrs)
	if err != nil {
		return err
	}

	err = t.setATRCompletedExclusive(ctx)
	if err != nil {
		if err.errorClass != TransactionErrorClassFailHard {
			return nil
		}

		return err
	}

	t.lock.Lock()
	t.state = TransactionAttemptStateCompleted
	t.lock.Unlock()

	return nil
}

func (t *transactionAttempt) commitStagedMutation(
	ctx context.Context,
	mutation *transactionStagedMutation,
) *TransactionOperationFailedError {
	err := t.fetchBeforeUnstage(ctx, mutation)
	if err != nil {
		return err
	}

	switch mutation.OpType {
	case TransactionStagedMutationInsert:
		return t.commitStagedInsert(ctx, *mutation, false)
	case TransactionStagedMutationReplace:
		return t.commitStagedReplace(ctx, *mutation, false, false)
	case TransactionStagedMutationRemove:
		return t.commitStagedRemove(ctx, *mutation, false)
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "unexpected staged mutation type")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailedPostCommit,
		})
	}
}

func (t *transactionAttempt) fetchBeforeUnstage(
	ctx context.Context,
	mutation *transactionStagedMutation,
) *TransactionOperationFailedError {
	ecCb := func(cerr *classifiedError) *TransactionOperationFailedError {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "fetching staged data failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		return t.operationFailed(operationFailedDef{
			Cerr:              cerr,
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailedPostCommit,
		})
	}

	if mutation.OpType != TransactionStagedMutationInsert && mutation.OpType != TransactionStagedMutationReplace {
		return ecCb(nil)
	}

	if mutation.Staged != nil {
		return ecCb(nil)
	}

	cerr := t.checkExpiredAtomic(ctx, hookCommitDoc, mutation.Key, false)
	if cerr != nil {
		t.setExpiryOvertimeAtomic()
	}

	var flags memdx.SubdocDocFlag
	if mutation.OpType == TransactionStagedMutationInsert {
		flags = memdx.SubdocDocFlagAccessDeleted
	}

	result, err := mutation.Agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags:      flags,
		OnBehalfOf: mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	if result.Ops[0].Err != nil {
		return ecCb(classifyError(result.Ops[0].Err))
	}

	var jsonTxn jsonTxnXattr
	err = json.Unmarshal(result.Ops[0].Value, &jsonTxn)
	if err != nil {
		return ecCb(classifyError(err))
	}

	if jsonTxn.ID.Attempt != t.id {
		return ecCb(classifyError(ErrOther))
	}

	mutation.Cas = result.Cas
	mutation.Staged = jsonTxn.Operation.Staged
	return nil
}

func (t *transactionAttempt) commitStagedReplace(
	ctx context.Context,
	mutation transactionStagedMutation,
	forceWrite bool,
	ambiguityResolution bool,
) *TransactionOperationFailedError {
	ecCb := func(cerr *classifiedError) *TransactionOperationFailedError {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing a replace failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				ambiguityResolution = true
				return t.commitStagedReplace(ctx, mutation, forceWrite, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocAlreadyExists:
			cerr.Class = TransactionErrorClassFailCasMismatch
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			if !ambiguityResolution {
				select {
				case <-time.After(3 * time.Millisecond):
					forceWrite = true
					return t.commitStagedReplace(ctx, mutation, forceWrite, ambiguityResolution)
				case <-ctx.Done():
					return t.contextFailed(ctx.Err())
				}
			}

			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		case TransactionErrorClassFailDocNotFound:
			return t.commitStagedInsert(ctx, mutation, ambiguityResolution)
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			select {
			case <-time.After(3 * time.Millisecond):
				return t.commitStagedReplace(ctx, mutation, forceWrite, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		default:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookCommitDoc, mutation.Key, false)
	if cerr != nil {
		t.setExpiryOvertimeAtomic()
	}

	err := t.hooks.BeforeDocCommitted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	cas := mutation.Cas
	if forceWrite {
		cas = 0
	}

	if mutation.Staged == nil {
		return ecCb(classifyError(
			wrapError(ErrIllegalState, "staged content is missing")))
	}

	result, err := mutation.Agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Cas:            cas,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
				Value: []byte("null"),
			},
			{
				Op:    memdx.MutateInOpTypeDelete,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.MutateInOpTypeSetDoc,
				Path:  nil,
				Value: mutation.Staged,
			},
		},
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		OnBehalfOf:      mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterDocCommittedBeforeSavingCAS(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	err = t.hooks.AfterDocCommitted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *transactionAttempt) commitStagedInsert(
	ctx context.Context,
	mutation transactionStagedMutation,
	ambiguityResolution bool,
) *TransactionOperationFailedError {
	ecCb := func(cerr *classifiedError) *TransactionOperationFailedError {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing an insert failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				ambiguityResolution = true
				return t.commitStagedInsert(ctx, mutation, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocAlreadyExists:
			cerr.Class = TransactionErrorClassFailCasMismatch
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			if !ambiguityResolution {
				select {
				case <-time.After(3 * time.Millisecond):
					return t.commitStagedReplace(ctx, mutation, true, ambiguityResolution)
				case <-ctx.Done():
					return t.contextFailed(ctx.Err())
				}
			}

			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			select {
			case <-time.After(3 * time.Millisecond):
				return t.commitStagedInsert(ctx, mutation, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		default:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookCommitDoc, mutation.Key, false)
	if cerr != nil {
		t.setExpiryOvertimeAtomic()
	}

	err := t.hooks.BeforeDocCommitted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	if mutation.Staged == nil {
		return ecCb(classifyError(
			wrapError(ErrIllegalState, "staged content is missing")))
	}

	_, err = mutation.Agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:       mutation.ScopeName,
		CollectionName:  mutation.CollectionName,
		Key:             mutation.Key,
		Value:           mutation.Staged,
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		OnBehalfOf:      mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	err = t.hooks.AfterDocCommittedBeforeSavingCAS(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	err = t.hooks.AfterDocCommitted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *transactionAttempt) commitStagedRemove(
	ctx context.Context,
	mutation transactionStagedMutation,
	ambiguityResolution bool,
) *TransactionOperationFailedError {
	ecCb := func(cerr *classifiedError) *TransactionOperationFailedError {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing a remove failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				ambiguityResolution = true
				return t.commitStagedRemove(ctx, mutation, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocNotFound:
			// Not finding the document during ambiguity resolution likely indicates
			// that it simply successfully performed the operation already. However, the mutation
			// token of that won't be available, so we need to just error it anyways :(
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			select {
			case <-time.After(3 * time.Millisecond):
				return t.commitStagedRemove(ctx, mutation, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		default:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookCommitDoc, mutation.Key, false)
	if cerr != nil {
		t.setExpiryOvertimeAtomic()
	}

	err := t.hooks.BeforeDocRemoved(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	_, err = mutation.Agent.Delete(ctx, &gocbcorex.DeleteOptions{
		ScopeName:       mutation.ScopeName,
		CollectionName:  mutation.CollectionName,
		Key:             mutation.Key,
		Cas:             0,
		DurabilityLevel: transactionsDurabilityLevelToMemdx(t.durabilityLevel),
		OnBehalfOf:      mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	err = t.hooks.AfterDocRemovedPreRetry(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	err = t.hooks.AfterDocRemovedPostRetry(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}
