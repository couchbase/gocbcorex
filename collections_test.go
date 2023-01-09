package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if pak.Command == memd.CmdCollectionsGetID && bytes.Equal(pak.Value, fqCollectionName) {
				pk := &memd.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				cb(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if pak.Command == memd.CmdCollectionsGetID {
				pk := &memd.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				if bytes.Equal(pak.Value, fqCollectionName1) {
					binary.BigEndian.PutUint32(pk.Extras[8:], cid1)
				} else if bytes.Equal(pak.Value, fqCollectionName2) {
					binary.BigEndian.PutUint32(pk.Extras[8:], cid2)
				}

				cb(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if pak.Command == memd.CmdCollectionsGetID && bytes.Equal(pak.Value, fqCollectionName) {
				cb(nil, cbErr)
				return
			}

			t.Error("Should not have reached here")
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		t.Error("Test should not have triggered a request")
		cb(nil, errors.New("bad"))
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			<-blockCh
			atomic.AddUint32(&called, 1)
			if pak.Command == memd.CmdCollectionsGetID && bytes.Equal(pak.Value, fqCollectionName) {
				assert.Equal(t, "endpoint1", endpoint)
				pk := &memd.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], cid)
				cb(pk, nil)
				return
			}

			t.Error("Should not have reached here")
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			t.Error("Should not have reached here")
			cb(nil, errors.New("nope"))
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		go func() {
			t.Error("Should not have reached here")
			cb(nil, errors.New("nope"))
		}()
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
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

	assert.Panics(t, func() {
		resolver.InvalidateCollectionID(&AsyncContext{}, scope, collection, "endpoint1", 4)
	})
}
