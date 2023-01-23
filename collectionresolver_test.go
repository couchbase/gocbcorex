package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type testCollectionResolver struct {
	err  error
	cid  uint32
	mRev uint64
}

func (tcr *testCollectionResolver) ResolveCollectionID(ctx context.Context, endpoint, scopeName, collectionName string) (collectionId uint32, manifestRev uint64, err error) {
	return collectionId, manifestRev, err
}

func (tcr *testCollectionResolver) InvalidateCollectionID(ctx context.Context, scopeName, collectionName, endpoint string, manifestRev uint64) {

}

// We expect to see a single get cid be dispatched for n requests, and then all the callbacks
// called with the cid.
func TestCollectionResolverCachedNCallsOneCollection(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	manifestRev := uint64(4)
	cid := uint32(9)

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.Equal(t, "", endpoint)
			assert.NotNil(t, ctx)

			return cid, manifestRev, nil
		}}
	resolver := NewCollectionResolverCached(mock)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func() {
			u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope, collection)
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
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, "", endpoint)
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
	resolver := NewCollectionResolverCached(mock)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func(i int) {
			if i%2 == 0 {
				u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope1, collection1)
				assert.Nil(t, err)
				assert.Equal(t, cid1, u)
				assert.Equal(t, manifestRev, mRev)
				wg.Done()
			} else {
				u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope2, collection2)
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
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cbErr := errors.New("some error")

	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.Equal(t, "", endpoint)
			assert.NotNil(t, ctx)

			return 0, 0, cbErr
		}}
	resolver := NewCollectionResolverCached(mock)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		go func() {
			u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope, collection)
			assert.Equal(t, cbErr, err)
			assert.Equal(t, uint32(0), u)
			assert.Equal(t, uint64(0), mRev)
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

func TestCollectionResolverCachedKnownCollection(t *testing.T) {
	scope := uuid.NewString()
	collection := uuid.NewString()
	manifestRev := uint64(4)
	fqCollectionName := scope + "." + collection
	cid := uint32(12)
	var called int
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			return 0, 0, errors.New("should not have reached here")
		}}

	resolver := NewCollectionResolverCached(mock)

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
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope, collection)
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
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(15)
	newCid := uint32(17)
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	manifestRev := uint64(4)
	newManifestRev := uint64(7)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			return newCid, newManifestRev, nil
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
		},
	}

	resolver := NewCollectionResolverCached(mock)

	manifest := &collectionsFastManifest{
		collections: map[string]collectionsFastCacheEntry{
			fqCollectionName: {
				CollectionID: cid,
				ManifestRev:  manifestRev,
			},
		},
	}
	resolver.slowMap = map[string]*collectionCacheEntry{
		fqCollectionName: {
			CollectionID: cid,
			ManifestRev:  manifestRev,
		},
	}
	resolver.fastCache.Store(manifest)

	resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 5)

	// We should have now invalidated the only entry in the cache.
	assert.Empty(t, resolver.fastCache.Load().collections)
	assert.Empty(t, resolver.slowMap)

	waitCh := make(chan struct{}, 1)
	go func() {
		u, mRev, err := resolver.ResolveCollectionID(context.Background(), "", scope, collection)
		assert.Nil(t, err)
		assert.Equal(t, newCid, u)
		assert.Equal(t, mRev, newManifestRev)
		waitCh <- struct{}{}
	}()
	<-waitCh

	assert.Equal(t, uint32(1), called)
}

//
// We expect an unknown collection with a older manifest rev to not send any requests.
func TestCollectionsResolverUnknownCollectionOlderManifestRev(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	cid := uint32(15)
	manifestRev := uint64(4)
	var called int
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			return 0, 0, errors.New("should be unreachable")
		},
		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
		},
	}

	resolver := NewCollectionResolverCached(mock)

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
	blockCh := make(chan struct{}, 1)
	cid := uint32(15)
	manifestRev := uint64(4)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.Equal(t, "", endpoint)
			assert.NotNil(t, ctx)

			return cid, manifestRev, nil
		},
	}

	resolver := NewCollectionResolverCached(mock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	waitCh := make(chan struct{}, 1)
	go func() {
		_, _, err := resolver.ResolveCollectionID(ctx, "", scope, collection)
		assert.Equal(t, context.Canceled, err)
		waitCh <- struct{}{}
	}()
	close(blockCh)
	<-waitCh

	assert.Equal(t, uint32(1), called)
}

//
func TestCollectionResolverCancelContextMultipleOps(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(15)
	manifestRev := uint64(4)
	mock := &CollectionResolverMock{
		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
			called++

			assert.Equal(t, scope, scopeName)
			assert.Equal(t, collection, collectionName)
			assert.Equal(t, "", endpoint)
			assert.NotNil(t, ctx)

			return cid, manifestRev, nil
		},
	}

	resolver := NewCollectionResolverCached(mock)

	var wg sync.WaitGroup
	numReqs := 10
	var toCancel []context.CancelFunc
	for i := 0; i < numReqs; i++ {
		wg.Add(1)

		var willCancel bool
		var ctx context.Context
		if i%2 == 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(context.Background())
			toCancel = append(toCancel, cancel)
			willCancel = true
		} else {
			ctx = context.Background()
		}

		go func(hasCanceled bool) {
			u, mRev, err := resolver.ResolveCollectionID(ctx, "", scope, collection)
			if willCancel {
				assert.Equal(t, context.Canceled, err)
			} else {
				assert.Equal(t, cid, u)
				assert.Equal(t, manifestRev, mRev)
			}
			wg.Done()
		}(willCancel)
	}

	for _, cancel := range toCancel {
		cancel()
	}

	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

// func TestCollectionsManagerCancelContextAllOps(t *testing.T) {
// 	var called uint32
// 	collection := uuid.NewString()
// 	scope := uuid.NewString()
// 	cid := uint32(9)
// 	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
// 	blockCh := make(chan struct{}, 1)
// 	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
// 		go func() {
// 			<-blockCh
// 			atomic.AddUint32(&called, 1)
// 			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
// 				pk := &memdx.Packet{
// 					Extras: make([]byte, 12),
// 				}
// 				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
// 				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
// 				blockCh <- struct{}{}
// 				handler(pk, nil)
// 				return
// 			}
//
// 			t.Error("Should not have reached here")
// 		}()
// 		return nil
// 	}
// 	client := &fakeKvClient{
// 		onCall: cliCb,
// 	}
// 	router := &fakeConnManager{
// 		cli: client,
// 	}
//
// 	resolver := newCollectionResolver(router)
//
// 	var wg sync.WaitGroup
// 	numReqs := 10
// 	ctx, cancel := context.WithCancel(context.Background())
// 	for i := 0; i < numReqs; i++ {
// 		wg.Add(1)
//
// 		go func() {
// 			_, _, err := resolver.ResolveCollectionID(ctx, "", scope, collection)
// 			assert.Equal(t, context.Canceled, err)
// 			wg.Done()
// 		}()
// 	}
// 	cancel()
//
// 	blockCh <- struct{}{}
// 	wg.Wait()
// 	<-blockCh
//
// 	assert.Equal(t, uint32(1), called)
// }
//
// func TestCollectionsManagerInvalidateTwice(t *testing.T) {
// 	var called uint32
// 	collection := uuid.NewString()
// 	scope := uuid.NewString()
// 	cid := uint32(15)
// 	manifestRev := uint64(4)
// 	fqCollectionName := scope + "." + collection
// 	mock := &CollectionResolverMock{
// 		ResolveCollectionIDFunc: func(ctx context.Context, endpoint string, scopeName string, collectionName string) (uint32, uint64, error) {
// 			called++
//
// 			assert.Equal(t, scope, scopeName)
// 			assert.Equal(t, collection, collectionName)
// 			assert.Equal(t, "", endpoint)
// 			assert.NotNil(t, ctx)
//
// 			return cid, manifestRev, nil
// 		},
// 		InvalidateCollectionIDFunc: func(ctx context.Context, scopeName string, collectionName string, endpoint string, manifestRev uint64) {
// 		},
// 	}
//
// 	resolver := NewCollectionResolverCached(mock)
// 	manifest := &collectionsFastManifest{
// 		collections: map[string]collectionsFastCacheEntry{
// 			fqCollectionName: {
// 				CollectionID: cid,
// 				ManifestRev:  manifestRev,
// 			},
// 		},
// 	}
// 	resolver.fastCache.Store(manifest)
// 	resolver.slowMap = map[string]*collectionCacheEntry{
// 		fqCollectionName: {
// 			CollectionID: cid,
// 			ManifestRev:  manifestRev,
// 		},
// 	}
//
// 	// Invalidate the collection ID and then allow the get cid fetch callback to be invoked at the same time as
// 	// a second invalidation.
// 	resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 5)
// 	go resolver.InvalidateCollectionID(context.Background(), scope, collection, "endpoint1", 6)
//
// 	waitCh := make(chan struct{}, 1)
// 	go func() {
// 		u, _, err := resolver.ResolveCollectionID(context.Background(), "", scope, collection)
// 		assert.Nil(t, err)
// 		assert.Equal(t, cid, u)
// 		waitCh <- struct{}{}
// 	}()
// 	<-waitCh
//
// 	assert.Equal(t, uint32(1), called)
// }
