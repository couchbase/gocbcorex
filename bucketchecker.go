package gocbcorex

import "context"

type BucketChecker interface {
	HasBucket(ctx context.Context, bucketName string) (bool, error)
}
