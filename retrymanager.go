package core

import (
	"context"
	"errors"
	"time"
)

type RetryController interface {
	ShouldRetry(err error) (time.Duration, bool)
}

type RetryManager interface {
	NewRetryController() RetryController
}

func OrchestrateMemdRetries[RespT any](
	ctx context.Context,
	rs RetryManager,
	fn func() (RespT, error),
) (RespT, error) {
	var opRetryController RetryController
	var lastErr error
	for {
		res, err := fn()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return res, retrierDeadline{err, lastErr}
			}

			if opRetryController == nil {
				opRetryController = rs.NewRetryController()
			}

			retryTime, shouldRetry := opRetryController.ShouldRetry(err)
			if shouldRetry {
				select {
				case <-time.After(retryTime):
				case <-ctx.Done():
					ctxErr := ctx.Err()
					if errors.Is(ctxErr, context.DeadlineExceeded) {
						return res, retrierDeadline{ctxErr, err}
					} else {
						return res, err
					}
				}
			}

			lastErr = err
			continue
		}

		return res, nil
	}
}
