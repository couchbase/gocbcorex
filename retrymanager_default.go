package gocbcorex

import (
	"context"
	"errors"
	"time"
)

type RetryManagerDefault struct {
	calc BackoffCalculator
}

func NewRetryManagerDefault() *RetryManagerDefault {
	return &RetryManagerDefault{
		calc: ExponentialBackoff(0, 500*time.Millisecond, 1),
	}
}

func (m *RetryManagerDefault) NewRetryController() RetryController {
	return &retryControllerDefault{
		parent: m,
	}
}

type retryControllerDefault struct {
	parent     *RetryManagerDefault
	retryCount uint32
}

func (rc *retryControllerDefault) isRetriableError(err error) bool {
	// Implement the default classification of retriable errors...
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return false
	}

	return true
}

func (rc *retryControllerDefault) ShouldRetry(err error) (time.Duration, bool) {
	if !rc.isRetriableError(err) {
		return 0, false
	}

	calc := rc.parent.calc

	// calculate the retry time for this attempt
	retryTime := calc(rc.retryCount)

	// increment the retry count
	rc.retryCount++

	return retryTime, true
}
