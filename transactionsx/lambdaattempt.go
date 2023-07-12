package transactionsx

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type TransactionLambdaAttempt struct {
	logger *zap.Logger
	txn    *Transaction
	txnErr error
	lock   sync.Mutex
}

func (a *TransactionLambdaAttempt) Get(ctx context.Context, opts TransactionGetOptions) (*TransactionGetResult, error) {
	result, err := a.txn.Get(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Insert(ctx context.Context, opts TransactionInsertOptions) (*TransactionGetResult, error) {
	result, err := a.txn.Insert(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Replace(ctx context.Context, opts TransactionReplaceOptions) (*TransactionGetResult, error) {
	result, err := a.txn.Replace(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Remove(ctx context.Context, opts TransactionRemoveOptions) (*TransactionGetResult, error) {
	result, err := a.txn.Remove(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) storeTxnErr(err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.txnErr == nil {
		a.txnErr = err
	}
}

type AttemptFunc func(context.Context, *TransactionLambdaAttempt) error

func (a *TransactionLambdaAttempt) run(ctx context.Context, attemptFn AttemptFunc) (*TransactionAttemptResult, error) {
	lambdaErr := attemptFn(ctx, a)

	a.lock.Lock()
	txnErr := a.txnErr
	a.lock.Unlock()

	if txnErr != nil {
		// if there was a transaction error, force that to be the processed err.
		// this could hide a user-error if they threw it after the transaction
		// failed, but this is expected.
		lambdaErr = txnErr
	}

	if lambdaErr != nil {
		result, rbErr := a.txn.Rollback(ctx)
		if rbErr != nil {
			a.logger.Error("failed to rollback errored transaction", zap.Error(rbErr))

			// we reuse the standard transaction error wrapper for this
			return nil, &TransactionPostErrorRollbackError{
				OriginalCause: lambdaErr,
				RollbackErr:   rbErr,
			}
		}

		return nil, &TransactionAttemptError{
			Cause:  lambdaErr,
			Result: result,
		}
	}

	result, err := a.txn.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}
