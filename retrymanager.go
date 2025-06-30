package gocbcorex

import (
	"context"
	"errors"
	"time"
)

type RetryController interface {
	ShouldRetry(ctx context.Context, err error) (time.Duration, bool, error)
}

type RetryManager interface {
	NewRetryController() RetryController
}

func OrchestrateRetries[RespT any](
	ctx context.Context,
	rs RetryManager,
	fn func() (RespT, error),
) (RespT, error) {
	var opRetryController RetryController
	var lastErr error
	for retryIdx := 0; ; retryIdx++ {
		res, err := fn()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return res, retrierDeadlineError{err, lastErr, retryIdx}
			}

			if opRetryController == nil {
				opRetryController = rs.NewRetryController()
			}

			retryTime, shouldRetry, orchErr := opRetryController.ShouldRetry(ctx, err)
			if orchErr != nil {
				return res, &RetryOrchestrationError{
					Cause:         orchErr,
					OriginalCause: err,
				}
			}

			if shouldRetry {
				select {
				case <-time.After(retryTime):
				case <-ctx.Done():
					ctxErr := ctx.Err()
					if errors.Is(ctxErr, context.DeadlineExceeded) {
						return res, retrierDeadlineError{ctxErr, err, retryIdx}
					} else {
						return res, err
					}
				}

				lastErr = err
				continue
			}

			return res, err
		}

		return res, nil
	}
}

func OrchestrateNoResponseRetries(
	ctx context.Context,
	rs RetryManager,
	fn func() error,
) error {
	_, err := OrchestrateRetries[struct{}](ctx, rs, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
