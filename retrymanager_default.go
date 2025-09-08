package gocbcorex

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/memdx"
)

type RetryManagerDefault struct {
	calc BackoffCalculator
}

func NewRetryManagerDefault() *RetryManagerDefault {
	return &RetryManagerDefault{
		calc: ExponentialBackoff(10*time.Millisecond, 500*time.Millisecond, 2),
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
	return errors.Is(err, memdx.ErrTmpFail) ||
		errors.Is(err, memdx.ErrConfigNotSet) ||
		errors.Is(err, memdx.ErrSyncWriteInProgress) ||
		errors.Is(err, memdx.ErrSyncWriteReCommitInProgress) ||
		errors.Is(err, cbhttpx.ErrConnectError) ||
		errors.Is(err, ErrVbucketMapOutdated) ||
		errors.Is(err, ErrCollectionManifestOutdated) ||
		errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned)
}

func (rc *retryControllerDefault) ShouldRetry(ctx context.Context, err error) (time.Duration, bool, error) {
	if !rc.isRetriableError(err) {
		return 0, false, nil
	}

	calc := rc.parent.calc

	// calculate the retry time for this attempt
	retryTime := calc(rc.retryCount)

	// increment the retry count
	rc.retryCount++

	return retryTime, true, nil
}
