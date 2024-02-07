package gocbcorex

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
)

type RetryManagerGetAllReplicas struct {
	calc BackoffCalculator
}

func NewRetryManagerGetAllReplicas() *RetryManagerGetAllReplicas {
	return &RetryManagerGetAllReplicas{
		calc: ExponentialBackoff(10*time.Millisecond, 500*time.Millisecond, 2),
	}
}

func (m *RetryManagerGetAllReplicas) NewRetryController() RetryController {
	return &retryControllerGetAllReplicas{
		parent: m,
	}
}

type retryControllerGetAllReplicas struct {
	parent     *RetryManagerGetAllReplicas
	retryCount uint32
}

func (rc *retryControllerGetAllReplicas) isRetriableError(err error) bool {
	// Implement the default classification of retriable errors...
	return errors.Is(err, memdx.ErrTmpFail) ||
		errors.Is(err, memdx.ErrConfigNotSet) ||
		errors.Is(err, memdx.ErrSyncWriteInProgress) ||
		errors.Is(err, memdx.ErrSyncWriteReCommitInProgress) ||
		errors.Is(err, ErrVbucketMapOutdated) ||
		errors.Is(err, ErrCollectionManifestOutdated) ||
		errors.Is(err, ErrNoServerAssigned) ||
		errors.Is(err, ErrRepeatedReplicaRead)
}

func (rc *retryControllerGetAllReplicas) ShouldRetry(ctx context.Context, err error) (time.Duration, bool, error) {
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
