package transactionsx

import (
	"context"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func (t *TransactionAttempt) Rollback(ctx context.Context) (*TransactionAttemptResult, error) {
	errSt := t.rollback(ctx)
	if errSt != nil {
		t.logger.Info("rollback failed", zap.Error(errSt.Err()))

		t.ensureCleanUpRequest()
		return nil, &TransactionAttemptError{
			Cause:  errSt.Err(),
			Result: t.result(),
		}
	}

	t.ensureCleanUpRequest()
	return t.result(), nil
}

func (t *TransactionAttempt) rollback(
	ctx context.Context,
) *TransactionOperationStatus {
	t.logger.Info("rolling back")

	t.lock.Lock()

	err := t.waitForOpsLocked(ctx)
	if err != nil {
		t.lock.Unlock()
		return err
	}

	err = t.checkCanRollbackLocked()
	if err != nil {
		t.lock.Unlock()
		return err
	}

	t.applyStateBits(transactionStateBitShouldNotCommit|transactionStateBitShouldNotRollback, 0)

	if t.state == TransactionAttemptStateNothingWritten {
		t.lock.Unlock()
		return nil
	}

	cerr := t.checkExpiredAtomic(ctx, hookRollback, nil, true)
	if cerr != nil {
		t.setExpiryOvertimeAtomic()
	}

	t.lock.Unlock()

	err = t.setATRAbortedExclusive(ctx)
	if err != nil {
		return err
	}

	t.lock.Lock()
	t.state = TransactionAttemptStateAborted
	t.lock.Unlock()

	var mutErrs []*TransactionOperationStatus
	if !t.enableParallelUnstaging {
		for _, mutation := range t.stagedMutations {
			err := t.unstageStagedMutation(ctx, mutation)
			if err != nil {
				mutErrs = append(mutErrs, err)
				break
			}
		}
	} else {
		numThreads := 32
		numMutations := len(t.stagedMutations)
		pendCh := make(chan *transactionStagedMutation, numThreads*2)
		waitCh := make(chan *TransactionOperationStatus, numMutations)

		// Start all our threads to process mutations
		for threadIdx := 0; threadIdx < numThreads; threadIdx++ {
			go func(threadIdx int) {
				for {
					mutation, ok := <-pendCh
					if !ok {
						break
					}

					err := t.unstageStagedMutation(ctx, mutation)
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

	err = t.setATRRolledBackExclusive(ctx)
	if err != nil {
		return err
	}

	t.lock.Lock()
	t.state = TransactionAttemptStateRolledBack
	t.lock.Unlock()

	return nil
}

func (t *TransactionAttempt) unstageStagedMutation(
	ctx context.Context,
	mutation *transactionStagedMutation,
) *TransactionOperationStatus {
	switch mutation.OpType {
	case TransactionStagedMutationInsert:
		return t.unstageStagedInsert(ctx, *mutation)
	case TransactionStagedMutationReplace:
		fallthrough
	case TransactionStagedMutationRemove:
		return t.unstageStagedRemoveReplace(ctx, *mutation)
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "unexpected staged mutation type")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
		})
	}
}

func (t *TransactionAttempt) unstageStagedInsert(
	ctx context.Context,
	mutation transactionStagedMutation,
) *TransactionOperationStatus {
	ecCb := func(cerr *classifiedError) *TransactionOperationStatus {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "removing a staged insert failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedInsert(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedInsert(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocNotFound:
			return nil
		case TransactionErrorClassFailPathNotFound:
			return nil
		case TransactionErrorClassFailDocAlreadyExists:
			cerr.Class = TransactionErrorClassFailCasMismatch
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		default:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedInsert(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookDeleteInserted, mutation.Key, true)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeRollbackDeleteInserted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	result, err := mutation.Agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Cas:            mutation.Cas,
		Flags:          memdx.SubdocDocFlagAccessDeleted,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpType(memdx.LookupInOpTypeGet),
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
				Value: []byte("null"),
			},
			{
				Op:    memdx.MutateInOpTypeDelete,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		OnBehalfOf: mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterRollbackDeleteInserted(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *TransactionAttempt) unstageStagedRemoveReplace(
	ctx context.Context,
	mutation transactionStagedMutation,
) *TransactionOperationStatus {
	ecCb := func(cerr *classifiedError) *TransactionOperationStatus {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "removing a staged remove or replace failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedRemoveReplace(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedRemoveReplace(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailPathNotFound:
			return nil
		case TransactionErrorClassFailDocNotFound:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailDocAlreadyExists:
			cerr.Class = TransactionErrorClassFailCasMismatch
			fallthrough
		case TransactionErrorClassFailCasMismatch:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		default:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.unstageStagedRemoveReplace(ctx, mutation)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookRollbackDoc, mutation.Key, true)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeDocRolledBack(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	result, err := mutation.Agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:      mutation.ScopeName,
		CollectionName: mutation.CollectionName,
		Key:            mutation.Key,
		Cas:            mutation.Cas,
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
		},
		OnBehalfOf: mutation.OboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterRollbackReplaceOrRemove(ctx, mutation.Key)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}
