package gocbcorex

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/memdx"
)

type RetryManagerConsistency struct {
	Base              RetryManager
	BucketChecker     BucketChecker
	CollectionChecker CollectionChecker
}

func (m *RetryManagerConsistency) NewRetryController() RetryController {
	return &retryControllerConsistency{
		parent: m,
		base:   m.Base.NewRetryController(),
	}
}

type retryControllerConsistency struct {
	parent *RetryManagerConsistency
	base   RetryController
}

func (rc *retryControllerConsistency) ShouldRetry(ctx context.Context, err error) (time.Duration, bool, error) {
	if errors.Is(err, memdx.ErrUnknownScopeName) {
		var memdResErr *memdx.ResourceError
		if errors.As(err, &memdResErr) {
			var bucketErr *KvBucketError
			if errors.As(err, &bucketErr) {
				exists, existsErr := rc.parent.CollectionChecker.HasScope(ctx,
					bucketErr.BucketName,
					memdResErr.ScopeName)
				if existsErr != nil {
					return 0, false, &contextualError{
						Message: "failed to double-check memd whether scope exists",
						Cause:   existsErr,
					}
				}

				if exists {
					return 500 * time.Millisecond, true, nil
				}
			} else {
				return 0, false, errors.New("failed to double-check memd whether scope exists due to missing bucket context")
			}
		} else {
			return 0, false, errors.New("failed to double-check memd whether scope exists due to missing resource context")
		}
	}

	if errors.Is(err, memdx.ErrUnknownCollectionName) {
		var memdResErr *memdx.ResourceError
		if errors.As(err, &memdResErr) {
			var bucketErr *KvBucketError
			if errors.As(err, &bucketErr) {
				exists, existsErr := rc.parent.CollectionChecker.HasCollection(ctx,
					bucketErr.BucketName,
					memdResErr.ScopeName,
					memdResErr.CollectionName)
				if existsErr != nil {
					return 0, false, &contextualError{
						Message: "failed to double-check memd whether collection exists",
						Cause:   existsErr,
					}
				}

				if exists {
					return 500 * time.Millisecond, true, nil
				}
			} else {
				return 0, false, errors.New("failed to double-check memd whether collection exists due to missing bucket context")
			}
		} else {
			return 0, false, errors.New("failed to double-check memd whether collection exists due to missing resource context")
		}
	}

	if errors.Is(err, cbqueryx.ErrBucketNotFound) {
		var queryResErr *cbqueryx.ResourceError
		if errors.As(err, &queryResErr) {
			exists, existsErr := rc.parent.BucketChecker.HasBucket(ctx,
				queryResErr.BucketName)
			if existsErr != nil {
				return 0, false, &contextualError{
					Message: "failed to double-check query whether bucket exists",
					Cause:   existsErr,
				}
			}

			if exists {
				return 500 * time.Millisecond, true, nil
			}
		} else {
			return 0, false, errors.New("failed to double-check query whether bucket exists due to missing resource context")
		}
	}

	if errors.Is(err, cbqueryx.ErrScopeNotFound) {
		var queryResErr *cbqueryx.ResourceError
		if errors.As(err, &queryResErr) {
			exists, existsErr := rc.parent.CollectionChecker.HasScope(ctx,
				queryResErr.BucketName,
				queryResErr.ScopeName)
			if existsErr != nil {
				return 0, false, &contextualError{
					Message: "failed to double-check query whether scope exists",
					Cause:   existsErr,
				}
			}

			if exists {
				return 500 * time.Millisecond, true, nil
			}
		} else {
			return 0, false, errors.New("failed to double-check query whether scope exists due to missing resource context")
		}
	}

	if errors.Is(err, cbqueryx.ErrCollectionNotFound) {
		var queryResErr *cbqueryx.ResourceError
		if errors.As(err, &queryResErr) {
			exists, existsErr := rc.parent.CollectionChecker.HasCollection(ctx,
				queryResErr.BucketName,
				queryResErr.ScopeName,
				queryResErr.CollectionName)
			if existsErr != nil {
				return 0, false, &contextualError{
					Message: "failed to double-check query whether collection exists",
					Cause:   existsErr,
				}
			}

			if exists {
				return 500 * time.Millisecond, true, nil
			}
		} else {
			return 0, false, errors.New("failed to double-check query whether collection exists due to missing resource context")
		}
	}

	return rc.base.ShouldRetry(ctx, err)
}
