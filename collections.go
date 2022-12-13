package core

type ResolveCollectionIDCallback func(ctx *asyncContext, collectionId uint32, manifestRev uint64, err error)

type CollectionResolver struct {
}

func (cr *CollectionResolver) ResolveCollectionID(ctx *asyncContext, scopeName, collectionName string, cb ResolveCollectionIDCallback) {

}

func (cr *CollectionResolver) InvalidateCollectionID(collectionID uint32, newManifestRev uint64) {

}

type CollectionManager struct {
	resolver *CollectionResolver
}

func (cm *CollectionManager) Dispatch(ctx *asyncContext, scopeName, collectionName string, dispatchCb func(*asyncContext, uint32, error, finalCallback), finalCb finalCallback) {
	cm.resolver.ResolveCollectionID(ctx, scopeName, collectionName, func(ctx *asyncContext, collectionId uint32, manifestRev uint64, err error) {
		dispatchCb(ctx, collectionId, err, finalCb)
	})
}
