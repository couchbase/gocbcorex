package gocbcorex

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type vbuuidBaggageKey struct{}

type VbucketUuidConsistency interface {
	VerifyVBucket(ctx context.Context, client KvClient, vbID uint16, vbUUID uint64) error
}

type vbucketConsistencyComponentOptions struct {
	logger *zap.Logger
}

type vbucketConsistencyComponent struct {
	logger *zap.Logger

	slowLock sync.Mutex
}

func newVbucketConsistencyComponent(opts *vbucketConsistencyComponentOptions) *vbucketConsistencyComponent {
	return &vbucketConsistencyComponent{
		logger: opts.logger,
	}
}

func (vb *vbucketConsistencyComponent) VerifyVBucket(ctx context.Context, client KvClient, vbID uint16, vbUUID uint64) error {
	if vbuuidCache, ok := client.Baggage(&vbuuidBaggageKey{}); ok {
		vc := vbuuidCache.(*vbucketUUIDCache)
		serverVbUUID, err := vc.LookupVbUUID(ctx, vbID, client)
		if err != nil {
			return err
		}

		if serverVbUUID != vbUUID {
			return VbucketUUIDMisMatchError{
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

	cache := newVBucketUUIDCache(&vbucketUUIDCacheOptions{})
	client.AddBaggage(&vbuuidBaggageKey{}, cache)
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
