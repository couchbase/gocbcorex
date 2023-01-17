package core

import (
	"github.com/couchbase/stellar-nebula/core/memdx"
	"sync"
)

type collectionInvalidation bool

const (
	collectionWasInvalidated    collectionInvalidation = true
	collectionWasNotInvalidated                        = false
)

type ResolveCollectionIDCallback func(collectionId uint32, manifestRev uint64, err error)

type collectionsManifestEntry struct {
	cid uint32
	rev uint64
}

type collectionsManifest struct {
	collections map[string]*collectionsManifestEntry
}

func makeManifestKey(scopeName, collectionName string) string {
	return scopeName + "." + collectionName
}

func (cm *collectionsManifest) Lookup(scopeName, collectionName string) (uint32, uint64, bool) {
	key := makeManifestKey(scopeName, collectionName)
	if entry, ok := cm.collections[key]; ok {
		return entry.cid, entry.rev, true
	}

	return 0, 0, false
}

type collectionIDCallbackNode struct {
	callback ResolveCollectionIDCallback
}

type perCidAwaitingDispatchItem struct {
	callbacks []*collectionIDCallbackNode
	ctx       *AsyncContext
	lock      sync.Mutex
}

type perCidCollectionResolver struct {
	manifest   AtomicPointer[collectionsManifest]
	dispatcher ConnectionManager

	// TODO: This isn't the correct data structure.
	awaitingDispatch     map[string]*perCidAwaitingDispatchItem
	awaitingDispatchLock sync.Mutex
}

func newCollectionResolver(dispatcher ConnectionManager) *perCidCollectionResolver {
	manifest := &collectionsManifest{
		collections: make(map[string]*collectionsManifestEntry),
	}
	cr := &perCidCollectionResolver{
		dispatcher:       dispatcher,
		awaitingDispatch: make(map[string]*perCidAwaitingDispatchItem),
	}
	cr.manifest.Store(manifest)

	return cr
}

func (cr *perCidCollectionResolver) loadManifest() *collectionsManifest {
	return cr.manifest.Load()
}

func (cr *perCidCollectionResolver) storeManifest(old, new *collectionsManifest) bool {
	return cr.manifest.CompareAndSwap(old, new)
}

func (cr *perCidCollectionResolver) refreshCid(ctx *AsyncContext, endpoint, key, scope, collection string) {
	cr.dispatcher.Execute(endpoint, func(client KvClient, err error) error {
		// TODO: DRY up or reconsider this error handling.
		if err != nil {
			cr.awaitingDispatchLock.Lock()
			pending, ok := cr.awaitingDispatch[key]
			if !ok {
				return nil
			}

			for _, cb := range pending.callbacks {
				cb.callback(0, 0, err)
			}
			delete(cr.awaitingDispatch, key)
			cr.awaitingDispatchLock.Unlock()
			return nil
		}

		return memdx.OpsUtils{
			ExtFramesEnabled: true, // TODO: ?
		}.GetCollectionID(client, &memdx.GetCollectionIDRequest{
			ScopeName:      scope,
			CollectionName: collection,
		}, func(resp *memdx.GetCollectionIDResponse, err error) {
			if err != nil {
				cr.awaitingDispatchLock.Lock()
				pending, ok := cr.awaitingDispatch[key]
				if !ok {
					return
				}

				for _, cb := range pending.callbacks {
					cb.callback(0, 0, err)
				}
				delete(cr.awaitingDispatch, key)
				cr.awaitingDispatchLock.Unlock()
				return
			}

			// Repeatedly try to apply the manifest in case someone else added or removed a cid whilst we've tried to
			// update.
			for {
				manifest := cr.loadManifest()

				// Create a new manifest containing all the old entries and the new one.
				manifestCollections := make(map[string]*collectionsManifestEntry, len(manifest.collections)-1)
				for k, cols := range manifest.collections {
					manifestCollections[k] = cols
				}
				manifestCollections[key] = &collectionsManifestEntry{
					cid: resp.CollectionID,
					rev: resp.ManifestID,
				}

				newManifest := &collectionsManifest{
					collections: manifestCollections,
				}

				if cr.storeManifest(manifest, newManifest) {
					break
				}
			}

			cr.awaitingDispatchLock.Lock()
			pending, ok := cr.awaitingDispatch[key]
			if !ok {
				cr.awaitingDispatchLock.Unlock()
				// This might be possible depending on how request cancellation works.
				return
			}
			delete(cr.awaitingDispatch, key)
			cr.awaitingDispatchLock.Unlock()

			pending.lock.Lock()
			callbacks := make([]ResolveCollectionIDCallback, len(pending.callbacks))
			for i, cb := range pending.callbacks {
				callbacks[i] = cb.callback
			}
			pending.callbacks = nil
			pending.lock.Unlock()

			for _, cb := range callbacks {
				cb(resp.CollectionID, resp.ManifestID, nil)
			}
		})
	})
}

func (cr *perCidCollectionResolver) ResolveCollectionID(ctx *AsyncContext, endpoint, scopeName, collectionName string, cb ResolveCollectionIDCallback) {
	// First try an atomic lookup to see if we already know about this collection.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		// TODO: This should be given some thought, this means that the callback is occurring in the same goroutine as the call.
		cb(cid, mRev, nil)
		return
	}

	key := makeManifestKey(scopeName, collectionName)

	// We don't know about this collection yet so check if there's a get cid already pending.
	cr.awaitingDispatchLock.Lock()
	// Try another lookup in case a pending get cid came in before we entered the lock.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		cr.awaitingDispatchLock.Unlock()
		// TODO: This should be given some thought, this means that the callback is occurring in the same goroutine as the call.
		cb(cid, mRev, nil)
		return
	}

	pending, ok := cr.awaitingDispatch[key]
	if !ok {
		// We don't have a pending get cid so create an entry in the map and send a request.
		pending = &perCidAwaitingDispatchItem{
			callbacks: []*collectionIDCallbackNode{},
			ctx:       NewAsyncContext(),
		}
		cr.awaitingDispatch[key] = pending
	}
	cr.awaitingDispatchLock.Unlock()

	cancelCtx := ctx.WithCancellation()

	handler := &collectionIDCallbackNode{
		callback: func(collectionId uint32, manifestRev uint64, err error) {
			if cancelCtx.MarkComplete() {
				cb(collectionId, manifestRev, err)
			}
		},
	}
	pending.lock.Lock()
	// We already sent a get cid request so just add this callback to the queue.
	pending.callbacks = append(pending.callbacks, handler)
	pending.lock.Unlock()

	cancelCtx.OnCancel(func(err error) bool {
		index := -1
		pending.lock.Lock()
		for i, callback := range pending.callbacks {
			if callback == handler {
				index = i
				break
			}
		}

		// Get cid request must have completed.
		if index == -1 {
			return false
		}

		pending.callbacks = append((pending.callbacks)[:index], (pending.callbacks)[index+1:]...)
		if len(pending.callbacks) == 0 {
			pending.ctx.Cancel()
			cr.awaitingDispatchLock.Lock()
			delete(cr.awaitingDispatch, key)
			cr.awaitingDispatchLock.Unlock()
		}
		pending.lock.Unlock()
		cb(0, 0, err)
		return true
	})

	if ok {
		return
	}

	// The collection is completely unknown, so we need to go to the server and see if it exists.
	cr.refreshCid(pending.ctx, endpoint, key, scopeName, collectionName)
}

func (cr *perCidCollectionResolver) InvalidateCollectionID(ctx *AsyncContext, scopeName, collectionName, endpoint string, newManifestRev uint64) {
	key := makeManifestKey(scopeName, collectionName)
	for {
		manifest := cr.loadManifest()
		entry, ok := manifest.collections[key]
		if !ok {
			// Somehow we're trying to invalidate a collection that we don't know about so just return.
			return
		}

		if entry.rev > newManifestRev {
			// The node that doesn't know about this collection is using an older manifest than us, so we don't want to
			// apply it.
			return
		} else if entry.rev == newManifestRev {
			panic("this shouldnt be possible")
		}

		// Build a new manifest without the invalid collection.
		manifestCollections := make(map[string]*collectionsManifestEntry, len(manifest.collections)-1)
		for k, cols := range manifest.collections {
			if k == key {
				continue
			}

			manifestCollections[k] = cols
		}

		newManifest := &collectionsManifest{
			collections: manifestCollections,
		}

		if cr.storeManifest(manifest, newManifest) {
			break
		}
	}

	cr.ResolveCollectionID(ctx, endpoint, scopeName, collectionName, func(collectionId uint32, manifestRev uint64, err error) {
	})
}

type CollectionResolver interface {
	ResolveCollectionID(ctx *AsyncContext, endpoint, scopeName, collectionName string, cb ResolveCollectionIDCallback)
	InvalidateCollectionID(ctx *AsyncContext, scopeName, collectionName, endpoint string, manifestRev uint64)
}
