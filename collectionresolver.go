package core

import "context"

type CollectionResolver interface {
	ResolveCollectionID(ctx context.Context, endpoint, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error)
	InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64)
}
