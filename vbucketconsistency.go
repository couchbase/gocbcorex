package gocbcorex

import (
	"context"
	"sync"
)

type vbuuidBaggageKey struct{}

type VbucketUuidConsistency interface {
	VerifyVBucket(ctx context.Context, client KvClient, vbID uint16, vbUUID uint64) error
}

type VbucketConsistencyComponent struct {
	slowLock sync.Mutex
}

func (vb *VbucketConsistencyComponent) VerifyVBucket(ctx context.Context, client KvClient, vbID uint16, vbUUID uint64) error {
	if vbuuidCache, ok := client.Baggage(&vbuuidBaggageKey{}); ok {
		vc := vbuuidCache.(*vbucketUUIDCache)
		serverVbUUID, err := vc.LookupVbUUID(ctx, vbID)
		if err != nil {
			return err
		}

		if serverVbUUID != vbUUID {
			return &VbucketUUIDMismatchError{
				RequestedVbId: vbID,
				RequestVbUUID: vbUUID,
				ActualVbUUID:  serverVbUUID,
			}
		}

		return nil
	}

	vb.slowLock.Lock()
	if _, ok := client.Baggage(&vbuuidBaggageKey{}); ok {
		vb.slowLock.Unlock()
		return vb.VerifyVBucket(ctx, client, vbID, vbUUID)
	}

	cache := newVBucketUUIDCache(&vbucketUUIDCacheOptions{
		Client: client,
	})
	client.SetBaggage(&vbuuidBaggageKey{}, cache)
	vb.slowLock.Unlock()

	return vb.VerifyVBucket(ctx, client, vbID, vbUUID)
}

func OrchestrateVBucketConsistency[RespT any](
	ctx context.Context,
	vb VbucketUuidConsistency,
	client KvClient,
	vbID uint16,
	vbUUID uint64,
	fn func(KvClient) (RespT, error),
) (RespT, error) {
	if vbUUID == 0 {
		return fn(client)
	}

	if err := vb.VerifyVBucket(ctx, client, vbID, vbUUID); err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	return fn(client)
}
