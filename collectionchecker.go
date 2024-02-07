package gocbcorex

import "context"

type CollectionChecker interface {
	HasScope(ctx context.Context, bucketName, scopeName string) (bool, error)
	HasCollection(ctx context.Context, bucketName, scopeName, collectionName string) (bool, error)
}
