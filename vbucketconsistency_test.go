package gocbcorex

import (
	"context"
	"strconv"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVBucketConsistencyFastCacheMatch(t *testing.T) {
	fastCache := vbucketUUIDCacheFastCache{
		vbuckets: map[uint16]uint64{
			1: 1234,
			2: 5678,
			3: 91011,
		},
	}
	cache := &vbucketUUIDCache{}
	cache.fastCache.Store(&fastCache)

	kvCli := &KvClientMock{
		BaggageFunc: func(key any) (any, bool) {
			return cache, true
		},
	}

	vbComp := &VbucketConsistencyComponent{}

	err := vbComp.VerifyVBucket(context.Background(), kvCli, 1, 1234)
	require.NoError(t, err)
}

func TestVBucketConsistencyFastCacheMismatch(t *testing.T) {
	fastCache := vbucketUUIDCacheFastCache{
		vbuckets: map[uint16]uint64{
			1: 1234,
			2: 5678,
			3: 91011,
		},
	}
	cache := &vbucketUUIDCache{}
	cache.fastCache.Store(&fastCache)

	kvCli := &KvClientMock{
		BaggageFunc: func(key any) (any, bool) {
			return cache, true
		},
	}

	vbComp := &VbucketConsistencyComponent{}

	err := vbComp.VerifyVBucket(context.Background(), kvCli, 1, 5678)
	require.ErrorIs(t, err, ErrVbucketUUIDMismatch)
}

func TestVBucketConsistencyFastCacheUnknownVbucket(t *testing.T) {
	fastCache := vbucketUUIDCacheFastCache{
		vbuckets: map[uint16]uint64{
			1: 1234,
			2: 5678,
			3: 91011,
		},
	}
	cache := &vbucketUUIDCache{}
	cache.fastCache.Store(&fastCache)

	kvCli := &KvClientMock{
		BaggageFunc: func(key any) (any, bool) {
			return cache, true
		},
	}

	vbComp := &VbucketConsistencyComponent{}

	err := vbComp.VerifyVBucket(context.Background(), kvCli, 5, 5678)
	require.ErrorIs(t, err, ErrInvalidVbucket)
}

func TestVBucketConsistencyFastCacheMissing(t *testing.T) {
	vbUUID := 170788223609151
	vbUUID2 := 225299882966157
	baggage := make(map[any]any)
	numBaggageCalls := 0
	numStatsCalls := 0

	kvCli := &KvClientMock{
		BaggageFunc: func(key any) (any, bool) {
			numBaggageCalls++
			if numBaggageCalls <= 2 {
				return nil, false
			}

			v, ok := baggage[key]

			return v, ok
		},
		SetBaggageFunc: func(key any, value any) {
			baggage[key] = value
		},
		StatsFunc: func(ctx context.Context, req *memdx.StatsRequest,
			dataCb func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error) {
			numStatsCalls++
			err := dataCb(&memdx.StatsDataResponse{
				Key:   "vb_1:uuid",
				Value: strconv.Itoa(vbUUID),
			})
			if err != nil {
				return nil, err
			}
			err = dataCb(&memdx.StatsDataResponse{
				Key:   "vb_2:uuid",
				Value: strconv.Itoa(vbUUID2),
			})
			if err != nil {
				return nil, err
			}

			return &memdx.StatsActionResponse{}, nil
		},
	}

	vbComp := &VbucketConsistencyComponent{}

	err := vbComp.VerifyVBucket(context.Background(), kvCli, 1, uint64(vbUUID))
	require.NoError(t, err)

	err = vbComp.VerifyVBucket(context.Background(), kvCli, 2, uint64(vbUUID2))
	require.NoError(t, err)

	// 4 calls, 2 checking baggage on first call, then checking baggage against after creating the cache.
	// 1 check on the second call.
	assert.Equal(t, 4, numBaggageCalls)
	assert.Equal(t, 1, numStatsCalls)
}

func TestVBucketConsistencyFastCacheMissBaggageRace(t *testing.T) {
	vbUUID := 170788223609151
	vbUUID2 := 225299882966157
	numBaggageCalls := 0
	numSetBaggageCalls := 0
	numStatsCalls := 0

	fastCache := vbucketUUIDCacheFastCache{
		vbuckets: map[uint16]uint64{
			1: 170788223609151,
			2: 225299882966157,
			3: 91011,
		},
	}
	cache := &vbucketUUIDCache{}
	cache.fastCache.Store(&fastCache)

	kvCli := &KvClientMock{
		BaggageFunc: func(key any) (any, bool) {
			numBaggageCalls++
			if numBaggageCalls == 1 {
				return nil, false
			}

			return cache, true
		},
		SetBaggageFunc: func(key any, value any) {
			numSetBaggageCalls++
		},
		StatsFunc: func(ctx context.Context, req *memdx.StatsRequest,
			dataCb func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error) {
			numStatsCalls++

			return &memdx.StatsActionResponse{}, nil
		},
	}

	vbComp := &VbucketConsistencyComponent{}

	err := vbComp.VerifyVBucket(context.Background(), kvCli, 1, uint64(vbUUID))
	require.NoError(t, err)

	err = vbComp.VerifyVBucket(context.Background(), kvCli, 2, uint64(vbUUID2))
	require.NoError(t, err)

	assert.Equal(t, 4, numBaggageCalls)
	assert.Zero(t, numStatsCalls)
	assert.Zero(t, numSetBaggageCalls)
}
