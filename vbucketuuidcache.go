package gocbcorex

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/memdx"
)

type vbucketUUIDCacheFastCache struct {
	vbuckets map[uint16]uint64
}

type vbucketUUIDCacheOptions struct {
}

type vbucketUUIDCache struct {
	fastCache atomic.Pointer[vbucketUUIDCacheFastCache]

	slowLock      sync.Mutex
	slowPendingCh chan struct{}
}

func newVBucketUUIDCache(opts *vbucketUUIDCacheOptions) *vbucketUUIDCache {
	return &vbucketUUIDCache{}
}

func (vc *vbucketUUIDCache) LookupVbUUID(ctx context.Context, vbID uint16, client KvClient) (uint64, error) {
	fastCache := vc.fastCache.Load()
	if fastCache != nil {
		if vbUUID, ok := fastCache.vbuckets[vbID]; ok {
			return vbUUID, nil
		} else {
			return 0, ErrInvalidVbucket
		}
	}

	vc.slowLock.Lock()
	if vc.slowPendingCh == nil {
		vc.slowPendingCh = make(chan struct{})
		vc.slowLock.Unlock()

		vbUUIDs, err := vc.getVBUUIDs(ctx, client)
		if err != nil {
			vc.slowLock.Lock()
			close(vc.slowPendingCh)
			vc.slowPendingCh = nil
			vc.slowLock.Unlock()
			return 0, err
		}

		vc.slowLock.Lock()
		close(vc.slowPendingCh)
		vc.slowPendingCh = nil

		fastCache := &vbucketUUIDCacheFastCache{
			vbuckets: vbUUIDs,
		}
		vc.fastCache.Store(fastCache)
		vc.slowLock.Unlock()

		return vc.LookupVbUUID(ctx, vbID, client)
	}

	pendingCh := vc.slowPendingCh
	vc.slowLock.Unlock()

	select {
	case <-pendingCh:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	return vc.LookupVbUUID(ctx, vbID, client)
}

func (vc *vbucketUUIDCache) getVBUUIDs(ctx context.Context, client KvClient) (map[uint16]uint64, error) {
	parser := memdx.VbucketSeqNoStatsParser{}
	_, err := client.Stats(ctx, &memdx.StatsRequest{
		UtilsRequestMeta: memdx.UtilsRequestMeta{
			OnBehalfOf: "",
		},
		GroupName: parser.GroupName(),
	}, func(response *memdx.StatsDataResponse) error {
		parser.HandleEntry(response.Key, response.Value)
		return nil
	})
	if err != nil {
		return nil, err
	}

	cache := make(map[uint16]uint64, len(parser.Vbuckets))
	for vbID, vbDetails := range parser.Vbuckets {
		cache[vbID] = vbDetails.Uuid
	}

	return cache, nil
}
