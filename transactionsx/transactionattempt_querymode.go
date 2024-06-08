package transactionsx

import (
	"context"
	"errors"
)

func (t *TransactionAttempt) operationNotImplemented() *TransactionOperationStatus {
	return t.operationFailed(operationFailedDef{
		Cerr:              classifyError(errors.New("not implemented")),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) queryModeBegin(ctx context.Context) *TransactionOperationStatus {
	t.lock.Lock()
	defer t.lock.Unlock()

	errSt := t.waitForOpsLocked(ctx)
	if errSt != nil {
		return errSt
	}

	t.isQueryMode = true

	return t.operationNotImplemented()
}

func (t *TransactionAttempt) queryGet(
	ctx context.Context,
	opts TransactionGetOptions,
) (*TransactionGetResult, *TransactionOperationStatus) {
	t.lock.Lock()

	// TODO(brett19): execute a query!

	return nil, t.operationFailed(operationFailedDef{
		Cerr:              classifyError(errors.New("not implemented")),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) queryInsert(
	ctx context.Context,
	opts TransactionInsertOptions,
) (*TransactionGetResult, *TransactionOperationStatus) {
	return nil, t.operationFailed(operationFailedDef{
		Cerr:              classifyError(errors.New("not implemented")),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) queryReplace(
	ctx context.Context,
	opts TransactionReplaceOptions,
) (*TransactionGetResult, *TransactionOperationStatus) {
	return nil, t.operationFailed(operationFailedDef{
		Cerr:              classifyError(errors.New("not implemented")),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) queryRemove(
	ctx context.Context,
	opts TransactionRemoveOptions,
) (*TransactionGetResult, *TransactionOperationStatus) {
	return nil, t.operationFailed(operationFailedDef{
		Cerr:              classifyError(errors.New("not implemented")),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) queryCommit(ctx context.Context) *TransactionOperationStatus {
	return t.operationNotImplemented()
}

func (t *TransactionAttempt) queryRollback(ctx context.Context) *TransactionOperationStatus {
	return t.operationNotImplemented()
}
