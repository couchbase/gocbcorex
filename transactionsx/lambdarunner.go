package transactionsx

import (
	"context"
	"errors"

	"go.uber.org/zap"
)

type LambdaRunner struct {
	Logger  *zap.Logger
	Manager *TransactionsManager
}

func (r *LambdaRunner) Run(ctx context.Context, perConfig *TransactionOptions, attemptFn AttemptFunc) (*TransactionLambdaResult, error) {
	txn, err := r.Manager.BeginTransaction(perConfig)
	if err != nil {
		return nil, err
	}

	result := &TransactionLambdaResult{
		TransactionID:     txn.ID(),
		Attempts:          nil,
		UnstagingComplete: true,
	}

	for {
		err := txn.NewAttempt()
		if err != nil {
			return nil, err
		}

		lambdaAttempt := &TransactionLambdaAttempt{
			logger: r.Logger,
			txn:    txn,
		}

		lambdaResult, err := lambdaAttempt.run(ctx, attemptFn)
		if err != nil {
			var txnErr *TransactionAttemptError
			if !errors.As(err, &txnErr) {
				// We cannot say anything about whether this is retryable, and
				// retrying it blindly would spin without bound (this path has
				// no expiry check and no backoff).  Treat it as fatal.
				r.Logger.Warn("unexpectedly fatal non-transaction error", zap.Error(err))
				return nil, &TransactionLambdaError{
					Cause:  err,
					Result: result,
				}
			}

			result.Attempts = append(result.Attempts, txnErr.Result)

			// Only a failure raised by the transaction itself is retryable.
			// An error the user's lambda returned is final -- retrying it
			// would just re-run the lambda until the transaction expires.
			if txnErr.FromTransaction && txn.ShouldRetry() {
				continue
			}

			return nil, &TransactionLambdaError{
				Cause:  err,
				Result: result,
			}
		}

		result.Attempts = append(result.Attempts, lambdaResult)
		result.UnstagingComplete = lambdaResult.UnstagingComplete
		return result, nil
	}
}
