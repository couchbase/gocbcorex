package gocbcorex

import (
	"context"
	"time"
)

type RetryManagerFastFail struct {
}

func NewRetryManagerFastFail() *RetryManagerFastFail {
	return &RetryManagerFastFail{}
}

func (m *RetryManagerFastFail) NewRetryController() RetryController {
	return &retryControllerFastFail{}
}

type retryControllerFastFail struct{}

func (rc retryControllerFastFail) ShouldRetry(ctx context.Context, err error) (time.Duration, bool, error) {
	return 0, false, nil
}
