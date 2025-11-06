package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/memdx"
)

var (
	ErrStillFetchingVbUuids = contextualDeadline{"still waiting for vbuuids to be fetched"}
)

type vbucketUUIDCacheFastCache struct {
	vbuckets map[uint16]uint64
}

type vbucketUUIDCacheEntry struct {
	PendingCh chan struct{}
	FetchErr  error
	VbUuids   map[uint16]uint64
}

type vbucketUUIDCacheOptions struct {
	Client KvClient
}

type vbucketUUIDCache struct {
	client KvClient

	fastCache atomic.Pointer[vbucketUUIDCacheFastCache]

	slowLock  sync.Mutex
	slowEntry *vbucketUUIDCacheEntry
}

func newVBucketUUIDCache(opts *vbucketUUIDCacheOptions) *vbucketUUIDCache {
	return &vbucketUUIDCache{
		client: opts.Client,
	}
}

func (vc *vbucketUUIDCache) LookupVbUUID(ctx context.Context, vbID uint16) (uint64, error) {
	fastCache := vc.fastCache.Load()
	if fastCache != nil {
		if vbUUID, ok := fastCache.vbuckets[vbID]; ok {
			return vbUUID, nil
		}

		return 0, ErrInvalidVbucket
	}

	return vc.lookupVbUUIDSlow(ctx, vbID)
}

func (vc *vbucketUUIDCache) lookupVbUUIDSlow(ctx context.Context, vbId uint16) (uint64, error) {
	vc.slowLock.Lock()

	slowEntry := vc.slowEntry
	if slowEntry == nil {
		slowEntry = &vbucketUUIDCacheEntry{
			PendingCh: make(chan struct{}),
		}
		vc.slowEntry = slowEntry

		go vc.fetchVbuuidsThread(slowEntry)
	}

	if slowEntry.PendingCh != nil {
		pendingCh := slowEntry.PendingCh

		vc.slowLock.Unlock()

		select {
		case <-pendingCh:
		case <-ctx.Done():
			ctxErr := ctx.Err()
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return 0, ErrStillFetchingVbUuids
			} else {
				return 0, ctxErr
			}
		}

		return vc.lookupVbUUIDSlow(ctx, vbId)
	}

	if slowEntry.FetchErr != nil {
		fetchErr := slowEntry.FetchErr
		vc.slowLock.Unlock()

		return 0, fetchErr
	}

	vbUuids := slowEntry.VbUuids
	vc.slowLock.Unlock()

	return vbUuids[vbId], nil
}

func (vc *vbucketUUIDCache) rebuildFastCacheLocked() {
	if vc.slowEntry == nil || vc.slowEntry.VbUuids == nil {
		vc.fastCache.Store(nil)
		return
	}

	fastCache := &vbucketUUIDCacheFastCache{
		vbuckets: vc.slowEntry.VbUuids,
	}
	vc.fastCache.Store(fastCache)
}

func (vc *vbucketUUIDCache) fetchVbuuidsThread(entry *vbucketUUIDCacheEntry) {
	// it's safe to read from the entry inside this function because our
	// thread 'owns' the entry throughout this function until we lock
	// in order to be able to write back to it.

	ctx := context.Background()

	parser := memdx.VbucketSeqNoStatsParser{}
	_, err := vc.client.Stats(ctx, &memdx.StatsRequest{
		UtilsRequestMeta: memdx.UtilsRequestMeta{
			OnBehalfOf: "",
		},
		GroupName: parser.GroupName(),
	}, func(response *memdx.StatsDataResponse) error {
		parser.HandleEntry(response.Key, response.Value)
		return nil
	})
	if err != nil {
		vc.slowLock.Lock()

		entry.FetchErr = err
		entry.VbUuids = nil

		pendingCh := entry.PendingCh
		entry.PendingCh = nil

		// Clear out the slow entry so that a future attempt will retry
		vc.slowEntry = nil
		vc.rebuildFastCacheLocked()

		vc.slowLock.Unlock()

		if pendingCh != nil {
			close(pendingCh)
		}

		return
	}

	vbUuids := make(map[uint16]uint64, len(parser.Vbuckets))
	for vbID, vbDetails := range parser.Vbuckets {
		vbUuids[vbID] = vbDetails.Uuid
	}

	vc.slowLock.Lock()

	entry.FetchErr = nil
	entry.VbUuids = vbUuids

	pendingCh := entry.PendingCh
	entry.PendingCh = nil

	// Keep this slow entry since its valid until the connection dies
	vc.rebuildFastCacheLocked()

	vc.slowLock.Unlock()

	if pendingCh != nil {
		close(pendingCh)
	}
}
