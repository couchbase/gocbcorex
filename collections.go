package core

import (
	"context"
	"github.com/couchbase/stellar-nebula/core/memdx"
	"sync"
	"sync/atomic"
)

type ResolveCollectionIDCallback func(collectionId uint32, manifestRev uint64, err error)

type CollectionResolver interface {
	ResolveCollectionID(ctx context.Context, endpoint, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error)
	InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64)
}

type collectionInvalidation bool

const (
	collectionWasInvalidated    collectionInvalidation = true
	collectionWasNotInvalidated                        = false
)

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
	cancel     context.CancelFunc
	numWaiters uint32
	ctx        context.Context
	signal     chan struct{}
	err        error
}

type perCidCollectionResolver struct {
	manifest   AtomicPointer[collectionsManifest]
	dispatcher ConnectionManager

	// TODO: This isn't the correct data structure.
	awaitingDispatch     map[string]*perCidAwaitingDispatchItem
	awaitingDispatchLock sync.Mutex
}

type perCidCollectionResolverOptions struct {
	Dispatcher ConnectionManager
}

func newPerCidCollectionResolver(opts perCidCollectionResolverOptions) *perCidCollectionResolver {
	return &perCidCollectionResolver{
		dispatcher:       opts.Dispatcher,
		awaitingDispatch: make(map[string]*perCidAwaitingDispatchItem),
	}
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

func (cr *perCidCollectionResolver) refreshCid(ctx context.Context, endpoint, key, scope, collection string) {
	errCh := make(chan error, 1)
	resCh := make(chan *memdx.GetCollectionIDResponse, 1)
	cr.dispatcher.Execute(endpoint, func(client KvClient, err error) error {
		// TODO: DRY up or reconsider this error handling.
		if err != nil {
			errCh <- err
			return err
		}

		return memdx.OpsUtils{
			ExtFramesEnabled: true, // TODO: ?
		}.GetCollectionID(client, &memdx.GetCollectionIDRequest{
			ScopeName:      scope,
			CollectionName: collection,
		}, func(resp *memdx.GetCollectionIDResponse, err error) {
			if err != nil {
				errCh <- err
				return
			}

			resCh <- resp
		})
	})

	select {
	case <-ctx.Done():
		cr.awaitingDispatchLock.Lock()
		pending, ok := cr.awaitingDispatch[key]
		if !ok {
			cr.awaitingDispatchLock.Unlock()
			return
		}

		pending.err = ctx.Err()
		close(pending.signal)

		delete(cr.awaitingDispatch, key)
		cr.awaitingDispatchLock.Unlock()
	case err := <-errCh:
		cr.awaitingDispatchLock.Lock()
		pending, ok := cr.awaitingDispatch[key]
		if !ok {
			cr.awaitingDispatchLock.Unlock()
			return
		}

		pending.err = err
		close(pending.signal)

		delete(cr.awaitingDispatch, key)
		cr.awaitingDispatchLock.Unlock()
	case resp := <-resCh:
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

		close(pending.signal)
	}
}

func (cr *perCidCollectionResolver) ResolveCollectionID(ctx context.Context, endpoint, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error) {
	// Special case the default scope and collection.
	if isDefaultScopeAndCollection(scopeName, collectionName) {
		return 0, 0, nil
	}
	// First try an atomic lookup to see if we already know about this collection.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		return cid, mRev, nil
	}

	key := makeManifestKey(scopeName, collectionName)

	// We don't know about this collection yet so check if there's a get cid already pending.
	cr.awaitingDispatchLock.Lock()
	// Try another lookup in case a pending get cid came in before we entered the lock.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		cr.awaitingDispatchLock.Unlock()
		return cid, mRev, nil
	}

	pending, ok := cr.awaitingDispatch[key]
	if !ok {
		// We don't have a pending get cid so create an entry in the map and send a request.
		refreshCtx, cancel := context.WithCancel(context.Background())
		pending = &perCidAwaitingDispatchItem{
			cancel: cancel,
			ctx:    refreshCtx,
			signal: make(chan struct{}),
		}
		cr.awaitingDispatch[key] = pending
		// The collection is completely unknown, so we need to go to the server and see if it exists.
		go cr.refreshCid(refreshCtx, endpoint, key, scopeName, collectionName)
	}
	cr.awaitingDispatchLock.Unlock()

	atomic.AddUint32(&pending.numWaiters, 1)
	select {
	case <-ctx.Done():
		// If this context has been canceled then we need to decrement the number of waiters,
		// and if there are no waiters then cancel the underlying get cid request.
		if atomic.AddUint32(&pending.numWaiters, ^uint32(0)) == 0 {
			pending.cancel()

			// Wait for the underlying op to cancel to prevent any races.
			<-pending.signal
		}

		return 0, 0, ctx.Err()
	case <-pending.signal:
		if pending.err != nil {
			return 0, 0, pending.err
		}

		// Start again.
		return cr.ResolveCollectionID(ctx, endpoint, scopeName, collectionName)
	}
}

func (cr *perCidCollectionResolver) InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, newManifestRev uint64) {
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

	go cr.ResolveCollectionID(ctx, endpoint, scopeName, collectionName)
}

func isDefaultScopeAndCollection(scopeName, collectionName string) bool {
	noCollection := collectionName == "" && scopeName == ""
	defaultCollection := collectionName == "_default" && scopeName == "_default"

	return noCollection || defaultCollection
}
