package core

import "time"

type RetryManagerFastFail struct {
}

func NewRetryManagerFastFail() *RetryManagerFastFail {
	return &RetryManagerFastFail{}
}

func (m *RetryManagerFastFail) NewRetryController() RetryController {
	return &retryControllerFastFail{}
}

type retryControllerFastFail struct{}

func (rc retryControllerFastFail) ShouldRetry(err error) (time.Duration, bool) {
	return 0, false
}
