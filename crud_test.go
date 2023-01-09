package core

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/stretchr/testify/assert"
)

type fakeVBucketDispatcher struct {
	delay    time.Duration
	endpoint string
}

func (f *fakeVBucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte) (string, error) {
	return f.endpoint, nil
}

func (f *fakeVBucketDispatcher) DispatchToVbucket(ctx *AsyncContext, vbID uint16) (string, error) {
	// TODO implement me. No, you can't make me.
	panic("implement me")
}

type fakeServerDispatcher struct {
	onCall func(*memd.Packet, string, func(*memd.Packet, error))
}

func (f *fakeServerDispatcher) DispatchToServer(ctx *AsyncContext, endpoint string, pak *memd.Packet, cb func(*memd.Packet, error)) error {
	f.onCall(pak, endpoint, cb)
	return nil
}

type fakeCollectionResolver struct {
	delay time.Duration
	err   error
	cid   uint32
	mRev  uint64
}

func (f *fakeCollectionResolver) ResolveCollectionID(ctx *AsyncContext, endpoint, scopeName, collectionName string, cb ResolveCollectionIDCallback) {
	time.AfterFunc(f.delay, func() {
		cb(f.cid, f.mRev, f.err)
	})
}

func (f *fakeCollectionResolver) InvalidateCollectionID(ctx *AsyncContext, scopeName, collectionName, endpoint string, newManifestRev uint64) {
}

type fakePacketResolver struct {
	err error
}

func (f *fakePacketResolver) ResolvePacket(*memd.Packet) error {
	return f.err
}

type fakeRetryOrch struct {
}

func (f *fakeRetryOrch) OrchestrateRetries(ctx *AsyncContext, dispatchCb func(func(error), error)) error {
	var handler func(error)
	handler = func(err error) {
		if err != nil {
			dispatchCb(nil, err)
			return
		}
		dispatchCb(handler, err)
	}
	dispatchCb(handler, nil)
	return nil
}

func TestAThing(t *testing.T) {
	t.SkipNow()

	var called int
	routerCb := func(_pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		time.AfterFunc(0, func() {
			if called == 3 {
				cb(&memd.Packet{}, nil)
				return
			}
			called++
			cb(nil, errors.New("rar"))
		})
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
	}
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter:  router,
		retries:       newRetryComponent(),
		errorResolver: &fakePacketResolver{},
	}

	waitCh := make(chan struct{}, 1)
	crud.Get(&AsyncContext{}, GetOptions{Key: []byte("hi")}, func(result *GetResult, err error) {
		assert.Nil(t, err)

		assert.Equal(t, 3, called)
		waitCh <- struct{}{}
	})
	<-waitCh
}

func TestCancellingAThing(t *testing.T) {
	t.SkipNow()

	ctx := &AsyncContext{}

	var called int
	routerCb := func(_pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		time.AfterFunc(0, func() {
			if called == 3 {
				ctx.Cancel()
			}
			called++
			cb(nil, errors.New("rar"))
		})
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
	}
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter:  router,
		retries:       newRetryComponent(),
		errorResolver: &fakePacketResolver{},
	}

	waitCh := make(chan struct{}, 1)
	crud.Get(ctx, GetOptions{Key: []byte("hi")}, func(result *GetResult, err error) {
		assert.ErrorIs(t, err, gocbcore.ErrRequestCanceled)

		assert.Equal(t, 4, called)
		waitCh <- struct{}{}
	})
	<-waitCh
}

func TestCollectionUnknownStandardResolver(t *testing.T) {
	var called int
	routerCb := func(pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		time.AfterFunc(0, func() {
			called++
			if pak.Command == memd.CmdCollectionsGetID {
				pk := &memd.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], 9)
				cb(pk, nil)
				return
			}

			cb(&memd.Packet{Value: []byte("value")}, nil)
		})
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
	}
	crud := &CrudComponent{
		collections: newCollectionResolver(router),
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter:  router,
		retries:       newRetryComponent(),
		errorResolver: &fakePacketResolver{},
	}

	waitCh := make(chan struct{}, 1)
	crud.Get(&AsyncContext{}, GetOptions{Key: []byte("hi"), ScopeName: "something", CollectionName: "else"}, func(result *GetResult, err error) {
		assert.Nil(t, err)

		assert.Equal(t, 2, called)
		waitCh <- struct{}{}
	})
	<-waitCh
}

func TestResolvingPacketWithError(t *testing.T) {
	ctx := &AsyncContext{}

	routerCb := func(_pak *memd.Packet, endpoint string, cb func(*memd.Packet, error)) {
		time.AfterFunc(0, func() {
			cb(&memd.Packet{
				Status: 1,
			}, nil)
		})
	}
	router := &fakeServerDispatcher{
		onCall: routerCb,
	}
	expectedErr := errors.New("some error")
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter: router,
		retries:      &fakeRetryOrch{},
		errorResolver: &fakePacketResolver{
			err: expectedErr,
		},
	}

	waitCh := make(chan struct{}, 1)
	crud.Get(ctx, GetOptions{Key: []byte("hi")}, func(result *GetResult, err error) {
		assert.Equal(t, expectedErr, err)

		waitCh <- struct{}{}
	})
	<-waitCh
}
