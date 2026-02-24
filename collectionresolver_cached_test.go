package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// We expect to see a single get cid be dispatched for n requests, and then all the callbacks
// called with the cid.
func TestCollectionResolverCachedNCallsOneCollection(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	manifestRev := uint64(4)
	cid := uint32(9)

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			return cid, manifestRev, nil
		}}
	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func() {
			u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
			assert.Nil(t, err)
			assert.Equal(t, cid, u)
			assert.Equal(t, manifestRev, mRev)
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

// We expect to see 2 get cids be dispatched for n requests, and then all the callbacks
// called with the cids.
func TestCollectionResolverNCallsNCollections(t *testing.T) {
	var called uint32
	collection1 := uuid.NewString()
	scope1 := uuid.NewString()
	collection2 := uuid.NewString()
	scope2 := uuid.NewString()
	cid1 := uint32(9)
	cid2 := uint32(10)
	manifestRev := uint64(4)

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			atomic.AddUint32(&called, 1)

			assert.NotNil(t, ctx)

			if scopeName == scope1 && collectionName == collection1 {
				return cid1, manifestRev, nil
			} else if scopeName == scope2 && collectionName == collection2 {
				return cid2, manifestRev, nil
			} else {
				t.Fatalf("Collection and scope par unknown: %s.%s", scopeName, collectionName)
			}
			return 0, 0, errors.New("code should be unreachable")
		}}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func(i int) {
			if i%2 == 0 {
				u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope1, collection1)
				assert.Nil(t, err)
				assert.Equal(t, cid1, u)
				assert.Equal(t, manifestRev, mRev)
				wg.Done()
			} else {
				u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope2, collection2)
				assert.Nil(t, err)
				assert.Equal(t, cid2, u)
				assert.Equal(t, manifestRev, mRev)
				wg.Done()
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint32(2), called)
}

func TestCollectionManagerDispatchErrors(t *testing.T) {
	var called atomic.Uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cbErr := errors.New("some error")

	// gate1/gate2 let the test synchronize with the first two mock
	// invocations. Any additional calls (from late-arriving goroutines
	// under scheduler pressure) pass through without blocking.
	gate1 := make(chan struct{})
	gate1Called := make(chan struct{})
	gate2 := make(chan struct{})
	gate2Called := make(chan struct{})

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			n := called.Add(1)

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			switch n {
			case 1:
				close(gate1Called)
				<-gate1
			case 2:
				close(gate2Called)
				<-gate2
			}

			return 0, 0, cbErr
		}}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numReqs := 10

	// Start goroutine 1 to create the entry and spawn the resolve
	// thread.
	wg.Add(1)
	go func() {
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
		assert.Equal(t, cbErr, err)
		assert.Equal(t, uint32(0), u)
		assert.Equal(t, uint64(0), mRev)
		wg.Done()
	}()

	// Wait for the first resolve thread to enter the mock. Its
	// time.Now() has already been captured.
	<-gate1Called

	// Start goroutines 2-10. Their time.Now() calls in
	// resolveCollectionIDSlow are guaranteed to return a time after
	// the resolve thread's requestedAt because we only start them
	// after the resolve thread is blocked inside the mock.
	for i := 1; i < numReqs; i++ {
		wg.Add(1)
		go func() {
			u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
			assert.Equal(t, cbErr, err)
			assert.Equal(t, uint32(0), u)
			assert.Equal(t, uint64(0), mRev)
			wg.Done()
		}()
	}

	// Give goroutines 2-10 time to enter resolveCollectionIDSlow
	// and wait on PendingCh. Since the resolve thread is still
	// blocked in the mock, PendingCh is open, so any goroutine
	// that has entered the slow path is waiting on it.
	time.Sleep(50 * time.Millisecond)

	// Unblock call 1. Waiting goroutines will see the error,
	// detect their requestedAt is after the entry's, and retry —
	// coalescing into a second resolution (call 2).
	close(gate1)

	// Wait for the coalesced retry to enter the mock, then unblock.
	<-gate2Called
	close(gate2)

	wg.Wait()

	// We expect at least 2 calls: the initial failure plus at least
	// one coalesced retry. Under scheduler pressure, a few extra
	// calls may occur from goroutines that hadn't entered the slow
	// path before call 1 completed.
	assert.GreaterOrEqual(t, called.Load(), uint32(2))
}

func TestCollectionResolverCachedKnownCollection(t *testing.T) {
	scope := uuid.NewString()
	collection := uuid.NewString()
	manifestRev := uint64(4)
	fqCollectionName := scope + "." + collection
	cid := uint32(12)
	var called int
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			return 0, 0, errors.New("should not have reached here")
		}}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	manifest := &collectionsFastManifest{
		collections: map[string]collectionsFastCacheEntry{
			fqCollectionName: {
				CollectionID: cid,
				ManifestRev:  manifestRev,
			},
		},
	}
	resolver.fastCache.Store(manifest)

	waitCh := make(chan struct{}, 1)
	go func() {
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
		assert.Nil(t, err)
		assert.Equal(t, uint32(12), u)
		assert.Equal(t, manifestRev, mRev)
		waitCh <- struct{}{}
	}()
	<-waitCh

	assert.Zero(t, called)
}

// We expect an unknown collection with a newer manifest rev to remove the collection from the cache, and send a get
// cid. Any further requests should get queued.
func TestCollectionResolverUnknownCollectionNewerManifestRev(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	baseCollectionId := uint32(17)
	baseManifestRev := uint64(7)
	resolveCount := 0
	invalidateCount := 0
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			resolveCount++
			return baseCollectionId + uint32(invalidateCount), baseManifestRev + uint64(invalidateCount), nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
			invalidateCount++
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	firstCid, firstManifestRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
	require.NoError(t, err)
	require.Equal(t, 1, resolveCount)
	require.Equal(t, baseCollectionId, firstCid)
	require.Equal(t, baseManifestRev, firstManifestRev)

	resolver.InvalidateCollectionID(context.Background(), scope, collection, "", 100)
	require.Equal(t, 1, invalidateCount)

	cid, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
	assert.Nil(t, err)
	assert.Equal(t, baseCollectionId+1, cid)
	assert.Equal(t, baseManifestRev+1, mRev)

	require.Equal(t, 2, resolveCount)
}

// We expect an unknown collection with a older manifest rev to not send any requests.
func TestCollectionsResolverUnknownCollectionOlderManifestRev(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	cid := uint32(15)
	manifestRev := uint64(4)
	var called int
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			return 0, 0, errors.New("should be unreachable")
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	manifest := &collectionsFastManifest{
		collections: map[string]collectionsFastCacheEntry{
			fqCollectionName: {
				CollectionID: cid,
				ManifestRev:  manifestRev,
			},
		},
	}
	resolver.fastCache.Store(manifest)
	resolver.slowMap = map[string]*collectionCacheEntry{
		fqCollectionName: {
			CollectionID: cid,
			ManifestRev:  manifestRev,
		},
	}

	resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 3)

	cols := resolver.fastCache.Load().collections
	assert.Len(t, cols, 1)
	assert.Equal(t, manifestRev, cols[fqCollectionName].ManifestRev)
	assert.Equal(t, cid, cols[fqCollectionName].CollectionID)
	slowMap := resolver.slowMap
	assert.Len(t, slowMap, 1)
	assert.Equal(t, manifestRev, slowMap[fqCollectionName].ManifestRev)
	assert.Equal(t, cid, slowMap[fqCollectionName].CollectionID)
}

// We expect an unknown collection with a older manifest rev to panic.
// func TestCollectionsManagerUnknownCollectionSameManifestRev(t *testing.T) {
// 	collection := uuid.NewString()
// 	scope := uuid.NewString()
// 	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
// 	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
// 		go func() {
// 			t.Error("Should not have reached here")
// 			handler(nil, errors.New("nope"))
// 		}()
// 		return nil
// 	}
// 	client := &fakeKvClient{
// 		onCall: cliCb,
// 	}
// 	router := &fakeConnManager{
// 		cli: client,
// 	}
// 	resolver := newCollectionResolver(router)
// 	manifest := &collectionsManifest{
// 		collections: map[string]*collectionsManifestEntry{
// 			fqCollectionName: {
// 				cid: 12,
// 				rev: 4,
// 			},
// 		},
// 	}
// 	resolver.storeManifest(resolver.loadManifest(), manifest)
//
// 	assert.Panics(t, func() {
// 		resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 4)
// 	})
// }

func TestCollectionResolverCancelContext(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	calledCh := make(chan struct{}, 1)
	blockCh := make(chan struct{}, 1)
	cid := uint32(15)
	manifestRev := uint64(4)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			called++
			close(calledCh)

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			<-blockCh

			return cid, manifestRev, nil
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = resolver.ResolveCollectionID(ctx, scope, collection)
	assert.Equal(t, context.Canceled, err)

	<-calledCh
	close(blockCh)

	assert.Equal(t, uint32(1), called)
}

func TestCollectionResolverTimeoutContext(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	calledCh := make(chan struct{}, 1)
	blockCh := make(chan struct{}, 1)
	cid := uint32(15)
	manifestRev := uint64(4)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			called++
			close(calledCh)

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			<-blockCh

			return cid, manifestRev, nil
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)

	_, _, err = resolver.ResolveCollectionID(ctx, scope, collection)
	cancel()
	assert.ErrorIs(t, err, ErrStillResolvingCollection)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	<-calledCh
	close(blockCh)

	assert.Equal(t, uint32(1), called)
}

func TestCollectionResolverCancelContextMultipleOps(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(15)
	manifestRev := uint64(4)
	blockCh := make(chan struct{}, 1)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			<-blockCh
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			return cid, manifestRev, nil
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	var cancelGroup sync.WaitGroup
	numReqs := 10
	var toCancel []context.CancelFunc
	for i := 0; i < numReqs; i++ {

		var willCancel bool
		var ctx context.Context
		if i%2 == 0 {
			cancelGroup.Add(1)
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(context.Background())
			toCancel = append(toCancel, cancel)
			willCancel = true
		} else {
			wg.Add(1)
			ctx = context.Background()
		}

		go func(hasCanceled bool) {
			u, mRev, err := resolver.ResolveCollectionID(ctx, scope, collection)
			if willCancel {
				assert.Equal(t, context.Canceled, err)
				cancelGroup.Done()
			} else {
				assert.Equal(t, cid, u)
				assert.Equal(t, manifestRev, mRev)
				wg.Done()
			}
		}(willCancel)
	}

	for _, cancel := range toCancel {
		cancel()
	}

	cancelGroup.Wait()

	close(blockCh)

	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerCancelContextAllOps(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(9)
	manifestRev := uint64(4)
	blockCh := make(chan struct{})
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			blockCh <- struct{}{}
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.NotNil(t, ctx)

			<-blockCh

			return cid, manifestRev, nil
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numReqs := 10
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < numReqs; i++ {
		wg.Add(1)

		go func() {
			_, _, err := resolver.ResolveCollectionID(ctx, scope, collection)
			assert.Equal(t, context.Canceled, err)
			wg.Done()
		}()
	}
	// Wait for the resolve collection id op to be sent.
	<-blockCh

	cancel()

	wg.Wait()

	// We block the resolve collection id op from completing until we've received a response
	// for all of our requests.
	blockCh <- struct{}{}

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerInvalidateTwice(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(15)
	manifestRev := uint64(4)

	var callCount atomic.Uint32
	// calledCh is signaled each time the mock is entered.
	// blockCh gates each resolver call so the test can control timing.
	calledCh := make(chan struct{}, 1)
	blockCh := make(chan struct{})
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			callCount.Add(1)
			calledCh <- struct{}{}
			<-blockCh
			return cid, manifestRev, nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	// Step 1: Resolve the collection for the first time to populate the cache.
	// The mock blocks until we signal, ensuring we control the timing.
	resultCh := make(chan error, 1)
	go func() {
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
		if err == nil {
			assert.Equal(t, cid, u)
			assert.Equal(t, manifestRev, mRev)
		}
		resultCh <- err
	}()

	// Wait for the resolver to be called, then unblock it.
	<-calledCh
	require.Equal(t, uint32(1), callCount.Load())
	blockCh <- struct{}{}

	require.NoError(t, <-resultCh)

	// Step 2: Invalidate the collection. This spawns a new resolve thread
	// that will block on blockCh (call #2).
	resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 5)

	// Wait for the re-resolve thread to call the resolver and block.
	<-calledCh
	require.Equal(t, uint32(2), callCount.Load())

	// Step 3: Invalidate again while the resolve thread is active.
	// Since DoneCh != nil, this should be a no-op.
	resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 6)

	// Unblock the re-resolve and verify the result.
	go func() {
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
		if err == nil {
			assert.Equal(t, cid, u)
			assert.Equal(t, manifestRev, mRev)
		}
		resultCh <- err
	}()

	blockCh <- struct{}{}
	require.NoError(t, <-resultCh)

	// The second invalidation should not have spawned a new resolve thread,
	// so the total call count should be exactly 2.
	require.Equal(t, uint32(2), callCount.Load())
}

// We expect that when a collection resolution fails (e.g. the collection does
// not exist), the error is returned to the caller. When a subsequent request
// comes in (e.g. after the collection has been created), it should trigger a
// fresh resolution rather than returning the stale cached error.
func TestCollectionResolverCachedRecoveryAfterError(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	cbErr := errors.New("unknown collection name")
	cid := uint32(9)
	manifestRev := uint64(4)

	var callCount atomic.Uint32
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string) (uint32, uint64, error) {
			call := callCount.Add(1)

			if call == 1 {
				return 0, 0, cbErr
			}

			return cid, manifestRev, nil
		},
	}

	resolver, err := NewCollectionResolverCached(&CollectionResolverCachedOptions{
		Resolver:       mock,
		ResolveTimeout: 10 * time.Second,
	})
	require.NoError(t, err)

	// First resolve triggers a resolution which will fail. The caller's
	// requestedAt predates the error, so this returns the error.
	_, _, err = resolver.ResolveCollectionID(context.Background(), scope, collection)
	require.ErrorIs(t, err, cbErr)

	// A subsequent resolve (whose requestedAt postdates the error) should
	// trigger a fresh resolution, which now succeeds.
	resolvedCid, resolvedManifestRev, err := resolver.ResolveCollectionID(context.Background(), scope, collection)
	require.NoError(t, err)
	require.Equal(t, cid, resolvedCid)
	require.Equal(t, manifestRev, resolvedManifestRev)
	require.Equal(t, uint32(2), callCount.Load())
}
