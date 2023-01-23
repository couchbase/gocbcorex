package core

import (
	"context"
	"sync"
)

type collectionsFastCacheEntry struct {
	CollectionID uint32
	ManifestRev  uint64
}

type collectionsFastManifest struct {
	collections map[string]collectionsFastCacheEntry
}

type collectionCacheEntry struct {
	ResolveErr   error
	CollectionID uint32
	ManifestRev  uint64

	PendingCh chan struct{}
}

type CollectionResolverCached struct {
	resolver CollectionResolver

	fastCache AtomicPointer[collectionsFastManifest]

	slowLock sync.Mutex
	slowMap  map[string]*collectionCacheEntry
}

var _ CollectionResolver = (*CollectionResolverCached)(nil)

func NewCollectionResolverCached(resolver CollectionResolver) *CollectionResolverCached {
	return &CollectionResolverCached{
		resolver: resolver,
		slowMap:  make(map[string]*collectionCacheEntry),
	}
}

func (cr *CollectionResolverCached) rebuildFastCacheLocked() {
	manifest := &collectionsFastManifest{
		collections: make(map[string]collectionsFastCacheEntry),
	}

	for fullKeyPath, entry := range cr.slowMap {
		if entry.PendingCh == nil && entry.CollectionID > 0 {
			manifest.collections[fullKeyPath] = collectionsFastCacheEntry{
				CollectionID: entry.CollectionID,
				ManifestRev:  entry.ManifestRev,
			}
		}
	}

	cr.fastCache.Store(manifest)
}

func (cr *CollectionResolverCached) ResolveCollectionID(
	ctx context.Context, endpoint, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	fullKeyPath := scopeName + "." + collectionName

	fastCache := cr.fastCache.Load()
	if fastCache != nil {
		collectionEntry, wasFound := fastCache.collections[fullKeyPath]
		if wasFound {
			return collectionEntry.CollectionID, collectionEntry.ManifestRev, nil
		}
	}

	return cr.resolveCollectionIdSlow(ctx, endpoint, scopeName, collectionName)
}

func (cr *CollectionResolverCached) resolveCollectionIdSlow(
	ctx context.Context, endpoint, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	// TODO(brett19): Actually implement collection resolver slowmap lookup
	/*
		cr.slowLock.Lock()
		slowEntry, hasSlowEntry := cr.slowMap[fullKeyPath]
		cr.slowLock.Unlock()
	*/

	return cr.resolver.ResolveCollectionID(ctx, endpoint, scopeName, collectionName)
}

func (cr *CollectionResolverCached) InvalidateCollectionID(
	ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64,
) {
	cr.resolver.InvalidateCollectionID(ctx, scopeName, collectionName, endpoint, manifestRev)
	fullKeyPath := scopeName + "." + collectionName

	cr.slowLock.Lock()

	slowEntry, wasFound := cr.slowMap[fullKeyPath]
	if !wasFound {
		// its not in the map anymore, we can leave early
		cr.slowLock.Unlock()
		return
	}

	if slowEntry.ManifestRev > manifestRev {
		// our entry is newer than is being invalidated, so leave it...
		cr.slowLock.Unlock()
		return
	}

	// TODO(brett19): Actually cancel the slow entry.
	slowEntry.CollectionID = 0
	delete(cr.slowMap, fullKeyPath)

	cr.rebuildFastCacheLocked()
	cr.slowLock.Unlock()
}
