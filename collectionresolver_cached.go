package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ErrStillResolvingCollection = contextualDeadline{"still waiting for collection resolution to finish"}
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

	// PendingCh represents a channel that can be listened to to know
	// when the collection resolution has been completed (with either
	// a success or a failure).
	PendingCh chan struct{}

	// DoneCh represents when the goroutine managing this entry has
	// completely shut down and stopped polling.  This only occurs
	// when the collection is successfully resolved, or the resolution
	// is cancelled.
	DoneCh chan struct{}
}

type CollectionResolverCachedOptions struct {
	Logger         *zap.Logger
	Resolver       CollectionResolver
	ResolveTimeout time.Duration
}

type CollectionResolverCached struct {
	logger         *zap.Logger
	resolver       CollectionResolver
	resolveTimeout time.Duration

	fastCache AtomicPointer[collectionsFastManifest]

	slowLock   sync.Mutex
	slowMap    map[string]*collectionCacheEntry
	clearSigCh chan struct{}
}

var _ CollectionResolver = (*CollectionResolverCached)(nil)

func NewCollectionResolverCached(opts *CollectionResolverCachedOptions) (*CollectionResolverCached, error) {
	if opts == nil {
		return nil, errors.New("options must be specified")
	}

	if opts.Resolver == nil {
		return nil, errors.New("resolver must be non-nil")
	}
	if opts.ResolveTimeout <= 0 {
		return nil, errors.New("resolveTimeout must be greater than 0")
	}

	return &CollectionResolverCached{
		logger:         loggerOrNop(opts.Logger),
		resolver:       opts.Resolver,
		resolveTimeout: opts.ResolveTimeout,
		slowMap:        make(map[string]*collectionCacheEntry),
		clearSigCh:     make(chan struct{}),
	}, nil
}

func (cr *CollectionResolverCached) rebuildFastCacheLocked() {
	manifest := &collectionsFastManifest{
		collections: make(map[string]collectionsFastCacheEntry),
	}

	for fullKeyPath, entry := range cr.slowMap {
		if entry.CollectionID > 0 {
			manifest.collections[fullKeyPath] = collectionsFastCacheEntry{
				CollectionID: entry.CollectionID,
				ManifestRev:  entry.ManifestRev,
			}
		}
	}

	cr.fastCache.Store(manifest)
}

func (cr *CollectionResolverCached) ResolveCollectionID(
	ctx context.Context, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	fullKeyPath := scopeName + "." + collectionName

	fastCache := cr.fastCache.Load()
	if fastCache != nil {
		collectionEntry, wasFound := fastCache.collections[fullKeyPath]
		if wasFound {
			return collectionEntry.CollectionID, collectionEntry.ManifestRev, nil
		}
	}

	return cr.resolveCollectionIDSlow(ctx, scopeName, collectionName)
}

func (cr *CollectionResolverCached) resolveCollectionThread(
	cancelSig <-chan struct{},
	entry *collectionCacheEntry,
	scopeName, collectionName string,
) {
	// it's safe to read from the entry inside this function because our
	// thread 'owns' the entry throughout this function until we lock
	// in order to be able to write back to it.

	cancelCtx, cancelFn := context.WithCancel(context.Background())
	go func() {
		select {
		case <-cancelSig:
			cancelFn()
		case <-cancelCtx.Done():
		}
	}()

	for {
		// before we start looping again, check if there is a context error which occurs
		// when this goroutine should be shut down
		if cancelCtx.Err() != nil {
			cr.slowLock.Lock()

			doneCh := entry.DoneCh
			entry.DoneCh = nil

			cr.slowLock.Unlock()

			close(doneCh)

			return
		}

		collectionID, manifestRev, err := cr.resolver.ResolveCollectionID(cancelCtx, scopeName, collectionName)
		cancelFn()

		if err != nil {
			cr.slowLock.Lock()

			entry.ResolveErr = err
			entry.CollectionID = 0

			pendingCh := entry.PendingCh
			entry.PendingCh = nil

			cr.slowLock.Unlock()

			if pendingCh != nil {
				close(pendingCh)
			}

			continue
		}

		cr.slowLock.Lock()

		entry.ResolveErr = nil
		entry.CollectionID = collectionID
		entry.ManifestRev = manifestRev

		doneCh := entry.DoneCh
		entry.DoneCh = nil

		pendingCh := entry.PendingCh
		entry.PendingCh = nil

		cr.slowLock.Unlock()

		if pendingCh != nil {
			close(pendingCh)
		}
		if doneCh != nil {
			close(doneCh)
		}

		break
	}
}

func (cr *CollectionResolverCached) resolveCollectionIDSlow(
	ctx context.Context, scopeName, collectionName string,
) (collectionId uint32, manifestRev uint64, err error) {
	fullKeyPath := scopeName + "." + collectionName

	cr.slowLock.Lock()

	slowEntry, hasSlowEntry := cr.slowMap[fullKeyPath]
	if !hasSlowEntry {
		slowEntry = &collectionCacheEntry{
			PendingCh: make(chan struct{}),
			DoneCh:    make(chan struct{}),
		}
		cr.slowMap[fullKeyPath] = slowEntry

		go cr.resolveCollectionThread(
			cr.clearSigCh, slowEntry, scopeName, collectionName)
	}

	if slowEntry.PendingCh != nil {
		pendingCh := slowEntry.PendingCh

		cr.slowLock.Unlock()

		select {
		case <-pendingCh:
		case <-ctx.Done():
			ctxErr := ctx.Err()
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return 0, 0, ErrStillResolvingCollection
			} else {
				return 0, 0, ctxErr
			}
		}

		return cr.resolveCollectionIDSlow(ctx, scopeName, collectionName)
	}

	if slowEntry.ResolveErr != nil {
		resolveErr := slowEntry.ResolveErr
		cr.slowLock.Unlock()

		return 0, 0, resolveErr
	}

	collectionID := slowEntry.CollectionID
	manifestRev = slowEntry.ManifestRev
	cr.slowLock.Unlock()

	return collectionID, manifestRev, nil
}

func (cr *CollectionResolverCached) InvalidateCollectionID(
	ctx context.Context,
	scopeName, collectionName string,
	invalidatingEndpoint string,
	invalidatingManifestRev uint64,
) {
	cr.resolver.InvalidateCollectionID(
		ctx, scopeName, collectionName, invalidatingEndpoint, invalidatingManifestRev)

	fullKeyPath := scopeName + "." + collectionName

	cr.slowLock.Lock()

	slowEntry, wasFound := cr.slowMap[fullKeyPath]
	if !wasFound {
		// its not in the map anymore, we can leave early
		cr.slowLock.Unlock()
		return
	}

	if slowEntry.DoneCh != nil {
		// our entry is already being refetched, no need to do anything
		cr.slowLock.Unlock()
		return
	}

	if slowEntry.ManifestRev > invalidatingManifestRev {
		// our entry is newer than is being invalidated, so leave it...
		cr.slowLock.Unlock()
		return
	}

	// Reset the entry
	slowEntry.ResolveErr = nil
	slowEntry.CollectionID = 0
	slowEntry.ManifestRev = 0

	if slowEntry.PendingCh == nil {
		slowEntry.PendingCh = make(chan struct{})
	}

	if slowEntry.DoneCh == nil {
		slowEntry.DoneCh = make(chan struct{})
		go cr.resolveCollectionThread(
			cr.clearSigCh, slowEntry, scopeName, collectionName)
	}

	cr.rebuildFastCacheLocked()
	cr.slowLock.Unlock()
}

func (cr *CollectionResolverCached) Clear() {
	cr.slowLock.Lock()

	clearSigCh := cr.clearSigCh
	cr.clearSigCh = make(chan struct{})

	slowMap := cr.slowMap
	cr.slowMap = make(map[string]*collectionCacheEntry)

	var pendingResolves []chan struct{}
	for _, slowEntry := range slowMap {
		if slowEntry.PendingCh != nil {
			pendingResolves = append(pendingResolves, slowEntry.PendingCh)
		}
	}

	cr.rebuildFastCacheLocked()

	cr.slowLock.Unlock()

	// close the clear signal to tell all goroutines to stop
	close(clearSigCh)

	// wait for all the goroutines to finish up
	for _, pendingCh := range pendingResolves {
		<-pendingCh
	}
}
