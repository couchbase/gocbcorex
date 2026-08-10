package transactionsx

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

type TransactionLambdaAttempt struct {
	logger *zap.Logger
	txn    *Transaction
	txnErr error
	lock   sync.Mutex
}

func (a *TransactionLambdaAttempt) Get(ctx context.Context, opts GetOptions) (*GetResult, error) {
	result, err := a.txn.Get(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Insert(ctx context.Context, opts InsertOptions) (*GetResult, error) {
	result, err := a.txn.Insert(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Replace(ctx context.Context, opts ReplaceOptions) (*GetResult, error) {
	result, err := a.txn.Replace(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

func (a *TransactionLambdaAttempt) Remove(ctx context.Context, opts RemoveOptions) (*GetResult, error) {
	result, err := a.txn.Remove(ctx, opts)
	if err != nil {
		a.storeTxnErr(err)
		return nil, err
	}

	return result, err
}

// HasExpired indicates whether the underlying transaction has expired.
//
// Exposed so that a lambda, or a hook it has configured, can observe the
// transaction's expiry state. Hooks receive only a context.Context and so have
// no other route to it.
func (a *TransactionLambdaAttempt) HasExpired() bool {
	return a.txn.HasExpired()
}

// TimeRemaining returns how long the underlying transaction has left before it
// expires.
func (a *TransactionLambdaAttempt) TimeRemaining() time.Duration {
	return a.txn.TimeRemaining()
}

func (a *TransactionLambdaAttempt) storeTxnErr(err error) {
	// Only failures that were fatal to the attempt are recorded. Err() returns
	// a *TransactionOperationError exactly in that case; anything else is a
	// benign failure the caller is free to handle, such as a get for a document
	// that does not exist. Recording those too would force the transaction to
	// fail even when the caller handled them and returned successfully.
	var opErr *TransactionOperationError
	if !errors.As(err, &opErr) {
		return
	}

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
		// A transaction failure takes precedence over whatever the lambda
		// returned, which may be a user error raised after the transaction had
		// already failed.  Hiding that user error is expected.
		//
		// It does not take precedence over another transaction failure though.
		// Once an operation has failed fatally, every later operation fails with
		// ErrPreviousOperationFailed, and if the lambda propagated one of those
		// then that is the error the application actually saw, and the one that
		// says where the attempt stopped.  Replacing it with the first recorded
		// failure reports an error the caller never observed.
		var lambdaOpErr *TransactionOperationError
		if !errors.As(lambdaErr, &lambdaOpErr) {
			lambdaErr = txnErr
		}
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
