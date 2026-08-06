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
		TransactionID: txn.ID(),
		Attempts:      nil,

		// Set from the successful attempt's result below.  Left false until
		// then, so that a transaction which never completes does not report
		// itself as having unstaged.
		UnstagingComplete: false,
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

			// Only a failure that was fatal to the attempt is retryable.
			//
			// transactionOperationStatus.Err() returns a
			// *TransactionOperationError precisely when the failure was fatal
			// (shouldRaise != Success); a benign failure such as a get for a
			// missing document is returned as its bare cause, and an error the
			// user's lambda produced is whatever they returned. In neither of
			// those cases does retrying help -- the lambda would just be re-run
			// to the same conclusion until the transaction expires.
			var opErr *TransactionOperationError
			if errors.As(txnErr.Cause, &opErr) && txn.ShouldRetry() {
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
