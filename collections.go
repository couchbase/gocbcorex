package core

type ResolveCollectionIDCallback func(collectionId uint32, manifestRev uint64, err error)

type CollectionResolver struct {
}

func (cr *CollectionResolver) ResolveCollectionID(ctx *AsyncContext, scopeName, collectionName string, cb ResolveCollectionIDCallback) {
	ctx.OnCancel(func(err error) {

	})

	ctx.DropOnCancel()
	cb(123, 2, nil)
}

func (cr *CollectionResolver) InvalidateCollectionID(ctx *AsyncContext, collectionID uint32, newManifestRev uint64) {

}

type CollectionManager interface {
	Dispatch(ctx *AsyncContext, scopeName, collectionName string, dispatchCb func(uint32, error))
}

type collectionManager struct {
	resolver *CollectionResolver
}

func (cm *collectionManager) Dispatch(ctx *AsyncContext, scopeName, collectionName string, dispatchCb func(uint32, error)) {
	cm.resolver.ResolveCollectionID(ctx, scopeName, collectionName, func(collectionId uint32, manifestRev uint64, err error) {
		dispatchCb(collectionId, err)
	})
}
