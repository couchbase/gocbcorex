package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/stellar-nebula/core/memdx"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// We expect to see a single get cid be dispatched for n requests, and then all the callbacks
// called with the cid.
func TestCollectionsManagerQueueMultipleCallbacksOneCollection(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(9)
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				handler(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}

	resolver := newCollectionResolver(router)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		resolver.ResolveCollectionID(&AsyncContext{}, "", scope, collection, func(u uint32, _ uint64, err error) {
			assert.Equal(t, cid, u)
			wg.Done()
		})
	}
	close(blockCh)
	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

// We expect to see 2 get cids be dispatched for n requests, and then all the callbacks
// called with the cids.
func TestCollectionsManagerQueueMultipleCallbacksNCollections(t *testing.T) {
	var called uint32
	collection1 := uuid.NewString()
	scope1 := uuid.NewString()
	collection2 := uuid.NewString()
	scope2 := uuid.NewString()
	cid1 := uint32(9)
	cid2 := uint32(10)
	fqCollectionName1 := []byte(fmt.Sprintf("%s.%s", scope1, collection1))
	fqCollectionName2 := []byte(fmt.Sprintf("%s.%s", scope2, collection2))
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				if bytes.Equal(req.Value, fqCollectionName1) {
					binary.BigEndian.PutUint32(pk.Extras[8:], cid1)
				} else if bytes.Equal(req.Value, fqCollectionName2) {
					binary.BigEndian.PutUint32(pk.Extras[8:], cid2)
				}

				handler(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		if i%2 == 0 {
			resolver.ResolveCollectionID(&AsyncContext{}, "", scope1, collection1, func(u uint32, _ uint64, err error) {
				assert.Equal(t, cid1, u)
				wg.Done()
			})
		} else {
			resolver.ResolveCollectionID(&AsyncContext{}, "", scope2, collection2, func(u uint32, _ uint64, err error) {
				assert.Equal(t, cid2, u)
				wg.Done()
			})
		}

	}
	close(blockCh)
	wg.Wait()

	assert.Equal(t, uint32(2), called)
}

func TestCollectionsManagerDispatchErrors(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	cbErr := errors.New("some error")
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				handler(nil, cbErr)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)

	var wg sync.WaitGroup
	numReqs := 10
	for i := 0; i < numReqs; i++ {
		wg.Add(1)
		resolver.ResolveCollectionID(&AsyncContext{}, "", scope, collection, func(u uint32, _ uint64, err error) {
			assert.Equal(t, cbErr, err)
			assert.Equal(t, uint32(0), u)
			wg.Done()
		})
	}
	close(blockCh)
	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerKnownCollection(t *testing.T) {
	scope := uuid.NewString()
	collection := uuid.NewString()
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		t.Error("Test should not have triggered a request")
		handler(nil, errors.New("bad"))
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)
	manifest := &collectionsManifest{
		collections: map[string]*collectionsManifestEntry{
			fqCollectionName: {
				cid: 12,
				rev: 4,
			},
		},
	}
	resolver.storeManifest(resolver.loadManifest(), manifest)

	waitCh := make(chan struct{}, 1)
	resolver.ResolveCollectionID(&AsyncContext{}, "", scope, collection, func(u uint32, _ uint64, err error) {
		assert.Equal(t, uint32(12), u)
		waitCh <- struct{}{}
	})
	<-waitCh
}

// We expect an unknown collection with a newer manifest rev to remove the collection from the cache, and send a get
// cid. Any further requests should get queued.
func TestCollectionsManagerUnknownCollectionNewerManifestRev(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(15)
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				// assert.Equal(t, "endpoint1", endpoint)
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				handler(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)
	manifest := &collectionsManifest{
		collections: map[string]*collectionsManifestEntry{
			string(fqCollectionName): {
				cid: 12,
				rev: 4,
			},
		},
	}
	resolver.storeManifest(resolver.loadManifest(), manifest)

	resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 5)

	// We should have now invalidated the only entry in the cache.
	assert.Empty(t, resolver.manifest.Load().collections)

	waitCh := make(chan struct{}, 1)
	resolver.ResolveCollectionID(&AsyncContext{}, "", scope, collection, func(u uint32, _ uint64, err error) {
		assert.Equal(t, cid, u)
		waitCh <- struct{}{}
	})
	close(blockCh)
	<-waitCh

	assert.Equal(t, uint32(1), called)
}

// We expect an unknown collection with a older manifest rev to not send any requests.
func TestCollectionsManagerUnknownCollectionOlderManifestRev(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			t.Error("Should not have reached here")
			handler(nil, errors.New("nope"))
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)
	manifest := &collectionsManifest{
		collections: map[string]*collectionsManifestEntry{
			string(fqCollectionName): {
				cid: 12,
				rev: 4,
			},
		},
	}
	resolver.storeManifest(resolver.loadManifest(), manifest)

	resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 3)

	cols := resolver.manifest.Load().collections
	assert.Len(t, cols, 1)
	assert.Equal(t, uint64(4), cols[fqCollectionName].rev)
	assert.Equal(t, uint32(12), cols[fqCollectionName].cid)
}

// We expect an unknown collection with a older manifest rev to panic.
func TestCollectionsManagerUnknownCollectionSameManifestRev(t *testing.T) {
	collection := uuid.NewString()
	scope := uuid.NewString()
	fqCollectionName := fmt.Sprintf("%s.%s", scope, collection)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			t.Error("Should not have reached here")
			handler(nil, errors.New("nope"))
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	resolver := newCollectionResolver(router)
	manifest := &collectionsManifest{
		collections: map[string]*collectionsManifestEntry{
			fqCollectionName: {
				cid: 12,
				rev: 4,
			},
		},
	}
	resolver.storeManifest(resolver.loadManifest(), manifest)

	assert.Panics(t, func() {
		resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 4)
	})
}

func TestCollectionsManagerCancelContext(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		called++
		pk := &memdx.Packet{
			Extras: make([]byte, 12),
		}
		binary.BigEndian.PutUint64(pk.Extras[0:], 4)
		binary.BigEndian.PutUint32(pk.Extras[8:], 7)

		handler(pk, nil)
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}

	resolver := newCollectionResolver(router)
	ctx := &AsyncContext{}
	ctx.Cancel()

	waitCh := make(chan struct{}, 1)
	resolver.ResolveCollectionID(ctx, "", scope, collection, func(u uint32, _ uint64, err error) {
		assert.Equal(t, context.Canceled, err)
		waitCh <- struct{}{}
	})
	close(blockCh)
	<-waitCh

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerCancelContextMultipleOps(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(9)
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				handler(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}

	resolver := newCollectionResolver(router)

	var wg sync.WaitGroup
	numReqs := 10
	var toCancel []*AsyncContext
	for i := 0; i < numReqs; i++ {
		wg.Add(1)

		var willCancel bool
		ctx := &AsyncContext{}
		if i%2 == 0 {
			toCancel = append(toCancel, ctx)
			willCancel = true
		}

		func(hasCanceled bool) {
			resolver.ResolveCollectionID(ctx, "", scope, collection, func(u uint32, _ uint64, err error) {
				if willCancel {
					assert.Equal(t, context.Canceled, err)
				} else {
					assert.Equal(t, cid, u)
				}
				wg.Done()
			})
		}(willCancel)
	}

	for _, ctx := range toCancel {
		ctx.Cancel()
	}

	close(blockCh)
	wg.Wait()

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerCancelContextAllOps(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(9)
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	blockCh := make(chan struct{}, 1)
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				handler(pk, nil)
				blockCh <- struct{}{}
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}

	resolver := newCollectionResolver(router)

	var wg sync.WaitGroup
	numReqs := 10
	ctx := &AsyncContext{}
	for i := 0; i < numReqs; i++ {
		wg.Add(1)

		resolver.ResolveCollectionID(ctx, "", scope, collection, func(u uint32, _ uint64, err error) {
			assert.Equal(t, context.Canceled, err)
			wg.Done()
		})
	}
	ctx.Cancel()

	blockCh <- struct{}{}
	wg.Wait()
	<-blockCh

	assert.Equal(t, uint32(1), called)
}

func TestCollectionsManagerInvalidateTwice(t *testing.T) {
	var called uint32
	collection := uuid.NewString()
	scope := uuid.NewString()
	cid := uint32(9)
	fqCollectionName := []byte(fmt.Sprintf("%s.%s", scope, collection))
	blockCh := make(chan struct{})
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if req.OpCode == memdx.OpCodeCollectionsGetID && bytes.Equal(req.Value, fqCollectionName) {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				blockCh <- struct{}{}
				handler(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}

	resolver := newCollectionResolver(router)
	manifest := &collectionsManifest{
		collections: map[string]*collectionsManifestEntry{
			string(fqCollectionName): {
				cid: 12,
				rev: 4,
			},
		},
	}
	resolver.storeManifest(resolver.loadManifest(), manifest)

	// Invalidate the collection ID and then allow the get cid fetch callback to be invoked at the same time as
	// a second invalidation.
	resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 5)
	resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 6)
	blockCh <- struct{}{}
	<-blockCh

	waitCh := make(chan struct{}, 1)
	resolver.ResolveCollectionID(&AsyncContext{}, "", scope, collection, func(u uint32, _ uint64, err error) {
		assert.Equal(t, cid, u)
		waitCh <- struct{}{}
	})
	<-waitCh

	assert.Equal(t, uint32(1), called)
}
