package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func (t *TransactionAttempt) Commit(ctx context.Context) (*TransactionAttemptResult, error) {
	t.logger.Info("committing")

	errSt := t.commit(ctx)
	if errSt != nil {
		t.logger.Info("commit failed", zap.Error(errSt.Err()))

		if t.shouldRollback() {
			if !t.isExpiryOvertimeAtomic() {
				t.applyStateBits(transactionStateBitPreExpiryAutoRollback, 0)
			}

			roErrSt := t.rollback(ctx)
			if roErrSt != nil {
				t.logger.Info("rollback after commit failure failed",
					zap.Error(roErrSt.Err()), zap.NamedError("commitError", errSt.Err()))

				t.ensureCleanUpRequest()
				return nil, &TransactionAttemptError{
					Cause: &TransactionPostErrorRollbackError{
						OriginalCause: errSt.Err(),
						RollbackErr:   roErrSt.Err(),
					},
					Result: t.result(),
				}
			}

			t.ensureCleanUpRequest()
			return nil, &TransactionAttemptError{
				Cause:  errSt.Err(),
				Result: t.result(),
			}
		}

		t.ensureCleanUpRequest()
		return nil, &TransactionAttemptError{
			Cause:  errSt.Err(),
			Result: t.result(),
		}
	}

	t.applyStateBits(transactionStateBitShouldNotRetry|transactionStateBitShouldNotRollback, 0)

	t.ensureCleanUpRequest()
	return t.result(), nil
}

func (t *TransactionAttempt) commit(
	ctx context.Context,
) *transactionOperationStatus {
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

	expired := t.checkExpiredAtomic(ctx, hookStageCommit, nil, false)
	if expired {
		t.lock.Unlock()
		return t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
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

	var mutErrs []*transactionOperationStatus
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
		pendCh := make(chan *stagedMutation, numThreads*2)
		waitCh := make(chan *transactionOperationStatus, numMutations)

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

func (t *TransactionAttempt) commitStagedMutation(
	ctx context.Context,
	mutation *stagedMutation,
) *transactionOperationStatus {
	err := t.fetchBeforeUnstage(ctx, mutation)
	if err != nil {
		return err
	}

	switch mutation.OpType {
	case StagedMutationInsert:
		return t.commitStagedInsert(ctx, *mutation, false)
	case StagedMutationReplace:
		return t.commitStagedReplace(ctx, *mutation, false, false)
	case StagedMutationRemove:
		return t.commitStagedRemove(ctx, *mutation)
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

func (t *TransactionAttempt) fetchBeforeUnstage(
	ctx context.Context,
	mutation *stagedMutation,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
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

	if mutation.OpType != StagedMutationInsert && mutation.OpType != StagedMutationReplace {
		return ecCb(nil)
	}

	if mutation.Staged != nil {
		return ecCb(nil)
	}

	expired := t.checkExpiredAtomic(ctx, hookStageCommitDoc, mutation.Key, false)
	if expired {
		t.setExpiryOvertimeAtomic()
	}

	var flags memdx.SubdocDocFlag
	if mutation.OpType == StagedMutationInsert {
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

	var jsonTxn TxnXattrJson
	err = json.Unmarshal(result.Ops[0].Value, &jsonTxn)
	if err != nil {
		return ecCb(classifyError(err))
	}

	if jsonTxn.ID.Attempt != t.id {
		return ecCb(classifyError(errors.New("transaction id mismatch during unstaging fetch")))
	}

	mutation.Cas = result.Cas
	mutation.Staged = jsonTxn.Operation.Staged
	return nil
}

func (t *TransactionAttempt) commitStagedReplace(
	ctx context.Context,
	mutation stagedMutation,
	forceWrite bool,
	ambiguityResolution bool,
) *transactionOperationStatus {
	expired := t.checkExpiredAtomic(ctx, hookStageCommitDoc, mutation.Key, false)
	if expired {
		t.setExpiryOvertimeAtomic()
	}

	err := invokeNoResHookWithDocID(ctx, t.hooks.DocCommitted, mutation.Key, func() error {
		cas := mutation.Cas
		if forceWrite {
			cas = 0
		}

		if mutation.Staged == nil {
			return wrapError(ErrIllegalState, "staged content is missing")
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			DurabilityLevel: memdDuraLevel,
			OnBehalfOf:      mutation.OboUser,
		})
		if err != nil {
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing a replace failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		cerr := classifyError(err)
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

	return nil
}

func (t *TransactionAttempt) commitStagedInsert(
	ctx context.Context,
	mutation stagedMutation,
	ambiguityResolution bool,
) *transactionOperationStatus {
	expired := t.checkExpiredAtomic(ctx, hookStageCommitDoc, mutation.Key, false)
	if expired {
		t.setExpiryOvertimeAtomic()
	}

	err := invokeNoResHookWithDocID(ctx, t.hooks.DocCommitted, mutation.Key, func() error {
		if mutation.Staged == nil {
			return wrapError(ErrIllegalState, "staged content is missing")
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
		}

		_, err = mutation.Agent.Add(ctx, &gocbcorex.AddOptions{
			ScopeName:       mutation.ScopeName,
			CollectionName:  mutation.CollectionName,
			Key:             mutation.Key,
			Value:           mutation.Staged,
			DurabilityLevel: memdDuraLevel,
			OnBehalfOf:      mutation.OboUser,
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing an insert failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		cerr := classifyError(err)
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

	return nil
}

func (t *TransactionAttempt) commitStagedRemove(
	ctx context.Context,
	mutation stagedMutation,
) *transactionOperationStatus {
	expired := t.checkExpiredAtomic(ctx, hookStageCommitDoc, mutation.Key, false)
	if expired {
		t.setExpiryOvertimeAtomic()
	}

	err := invokeNoResHookWithDocID(ctx, t.hooks.DocRemoved, mutation.Key, func() error {
		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
		}

		_, err = mutation.Agent.Delete(ctx, &gocbcorex.DeleteOptions{
			ScopeName:       mutation.ScopeName,
			CollectionName:  mutation.CollectionName,
			Key:             mutation.Key,
			Cas:             0,
			DurabilityLevel: memdDuraLevel,
			OnBehalfOf:      mutation.OboUser,
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "committing a remove failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.commitStagedRemove(ctx, mutation)
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

	return nil
}
