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
				r.Logger.Warn("unexpectedly fatal non-transaction error")
				continue
			}

			result.Attempts = append(result.Attempts, txnErr.Result)

			if txn.ShouldRetry() {
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
