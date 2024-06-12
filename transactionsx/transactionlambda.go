package transactionsx

import (
	"context"

	"go.uber.org/zap"
)

type LambdaRunner struct {
	Logger  *zap.Logger
	Manager *TransactionsManager
}

type AttemptFunc func(context.Context, *TransactionAttempt) error

func (r *LambdaRunner) Run(ctx context.Context, perConfig *TransactionOptions, attemptFn AttemptFunc) (*TransactionResult, error) {
	txn, err := r.Manager.BeginTransaction(perConfig)
	if err != nil {
		return nil, err
	}

	var finalErr error
	for {
		err := txn.NewAttempt()
		if err != nil {
			finalErr = err
			break
		}

		lambdaErr := attemptFn(ctx, txn.attempt)
		if lambdaErr != nil {
			/*
				var tofErr *TransactionOperationStatus
				if !errors.As(lambdaErr, &tofErr) {
					// if this was a user-generated error, we should abort the transaction

					if txn.ShouldRollback() {
						// we intentionally ignore the error here since we are already in an error state,
						// we also assume that the transaction itself has logged that something went
						// wrong when performing the rollback.
						_ = txn.Rollback(ctx)
					}

					finalErr = lambdaErr
					break
				}
			*/
		}

		if txn.CanCommit() {
			err := txn.Commit(ctx)
			if err != nil {
				if txn.ShouldRetry() {
					continue
				}

				finalErr = err
				break
			}
		} else if txn.ShouldRollback() {
			err := txn.Rollback(ctx)
			if err != nil {
				if txn.ShouldRetry() {
					continue
				}

				finalErr = err
				break
			}
		} else {
			// TODO(brett19): What the hell?  User must have swallowed our error I guess?
		}
	}

	result := &TransactionResult{}

	if finalErr != nil {
		return result, finalErr
	}

	return result, nil
}
