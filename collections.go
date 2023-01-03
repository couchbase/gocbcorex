package core

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/gocbcore/v10/memd"
	"sync"
	"sync/atomic"
)

type collectionInvalidation bool

const (
	collectionWasInvalidated    collectionInvalidation = true
	collectionWasNotInvalidated                        = false
)

type ResolveCollectionIDCallback func(collectionId uint32, manifestRev uint64, err error)

type collectionsManifestEntry struct {
	cid  uint32
	rev  uint64
	lock sync.Mutex
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

type collectionResolver struct {
	manifest   atomic.Value
	dispatcher ServerDispatcher

	// TODO: This isn't the correct data structure.
	awaitingDispatch     map[string][]ResolveCollectionIDCallback
	awaitingDispatchLock sync.Mutex
}

func newCollectionResolver(dispatcher ServerDispatcher) *collectionResolver {
	manifest := &collectionsManifest{
		collections: make(map[string]*collectionsManifestEntry),
	}
	cr := &collectionResolver{
		dispatcher:       dispatcher,
		awaitingDispatch: make(map[string][]ResolveCollectionIDCallback),
	}
	cr.manifest.Store(manifest)

	return cr
}

func (cr *collectionResolver) loadManifest() *collectionsManifest {
	return cr.manifest.Load().(*collectionsManifest)
}

func (cr *collectionResolver) storeManifest(old, new *collectionsManifest) bool {
	return cr.manifest.CompareAndSwap(old, new)
}

func (cr *collectionResolver) refreshCid(ctx *AsyncContext, endpoint, key string, packet *memd.Packet) {
	cr.dispatcher.DispatchToServer(ctx, endpoint, packet, func(resp *memd.Packet, err error) {
		// TODO: If this is a timeout then we'll have to recurse using the context at the top of the queue.
		if err != nil {
			cr.awaitingDispatchLock.Lock()
			pending, ok := cr.awaitingDispatch[key]
			if !ok {
				return
			}

			for _, cb := range pending {
				cb(0, 0, err)
			}
			delete(cr.awaitingDispatch, key)
			cr.awaitingDispatchLock.Unlock()
		}

		// Pull the details out of the packet.
		manifestRev := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

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
				cid: collectionID,
				rev: manifestRev,
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
			// This might be possible depending on how request cancellation works.
		}

		var callbacks []ResolveCollectionIDCallback
		for _, cb := range pending {
			callbacks = append(callbacks, cb)
		}
		delete(cr.awaitingDispatch, key)
		cr.awaitingDispatchLock.Unlock()

		for _, cb := range callbacks {
			cb(collectionID, manifestRev, nil)
		}

	})
}

func (cr *collectionResolver) ResolveCollectionID(ctx *AsyncContext, endpoint, scopeName, collectionName string, cb ResolveCollectionIDCallback) {
	// First try an atomic lookup to see if we already know about this collection.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		// TODO: This should be given some thought, this means that the callback is occurring in the same goroutine as the call.
		cb(cid, mRev, nil)
	}

	key := makeManifestKey(scopeName, collectionName)

	// We don't know about this collection yet so check if there's a get cid already pending.
	cr.awaitingDispatchLock.Lock()
	// Try another lookup in case a pending get cid came in before we entered the lock.
	if cid, mRev, ok := cr.loadManifest().Lookup(scopeName, collectionName); ok {
		// TODO: This should be given some thought, this means that the callback is occurring in the same goroutine as the call.
		cb(cid, mRev, nil)
	}

	pending, ok := cr.awaitingDispatch[key]
	if ok {
		// We already sent a get cid request so just add this callback to the queue.
		pending = append(pending, cb)
		cr.awaitingDispatchLock.Unlock()
		return
	}
	// We don't have a pending get cid so create an entry in the map and send a request.
	cr.awaitingDispatch[key] = []ResolveCollectionIDCallback{cb}
	cr.awaitingDispatchLock.Unlock()

	// The collection is completely unknown, so we need to go to the server and see if it exists.
	packet := &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdCollectionsGetID,
		Datatype: 0,
		Cas:      0,
		Extras:   nil,
		Key:      nil,
		Value:    []byte(fmt.Sprintf("%s.%s", scopeName, collectionName)),
	}

	cr.refreshCid(ctx, endpoint, key, packet)
}

func (cr *collectionResolver) InvalidateCollectionID(scopeName, collectionName string, newManifestRev uint64) collectionInvalidation {
	key := makeManifestKey(scopeName, collectionName)
	for {
		manifest := cr.loadManifest()
		entry, ok := manifest.collections[key]
		if !ok {
			// Somehow we're trying to invalidate a collection that we don't know about so just return.
			return collectionWasNotInvalidated
		}

		if entry.rev > newManifestRev {
			// The node that doesn't know about this collection is using an older manifest than us, so we don't want to
			// apply it.
			return collectionWasNotInvalidated
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
			return collectionWasInvalidated
		}
	}
}

type CollectionManager interface {
	Dispatch(ctx *AsyncContext, scopeName, collectionName string, dispatchCb func(uint32, error))
}

type collectionManager struct {
	resolver *collectionResolver
}

func (cm *collectionManager) Dispatch(ctx *AsyncContext, scopeName, collectionName string, dispatchCb func(uint32, error)) {
	cm.resolver.ResolveCollectionID(ctx, "", scopeName, collectionName, func(collectionId uint32, manifestRev uint64, err error) {
		if err != nil {
			dispatchCb(0, err)
			return
		}

		dispatchCb(collectionId, nil)
	})
}

func (cm *collectionManager) CollectionIsUnknown(ctx *AsyncContext, endpoint, scopeName, collectionName string, manifestRev uint64) {
	if cm.resolver.InvalidateCollectionID(scopeName, collectionName, manifestRev) {
		cm.resolver.ResolveCollectionID(ctx, endpoint, scopeName, collectionName, func(collectionId uint32, manifestRev uint64, err error) {

		})
	}
}
