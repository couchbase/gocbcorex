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
	err      error
	endpoint string
}

func (f *fakeVBucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte) (string, error) {
	return f.endpoint, f.err
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

type fakeCollectionManager struct {
	delay time.Duration
	err   error
	cid   uint32
}

func (f *fakeCollectionManager) Dispatch(ctx *AsyncContext, scopeName, collectionName string, cb func(uint32, error)) {
	time.AfterFunc(f.delay, func() {
		cb(f.cid, f.err)
	})
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
		collections: &fakeCollectionManager{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter: router,
		retries:      newRetryComponent(),
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
		collections: &fakeCollectionManager{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter: router,
		retries:      newRetryComponent(),
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
		collections: &collectionManager{
			resolver: newCollectionResolver(router),
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		serverRouter: router,
		retries:      newRetryComponent(),
	}

	waitCh := make(chan struct{}, 1)
	crud.Get(&AsyncContext{}, GetOptions{Key: []byte("hi"), ScopeName: "something", CollectionName: "else"}, func(result *GetResult, err error) {
		assert.Nil(t, err)

		assert.Equal(t, 2, called)
		waitCh <- struct{}{}
	})
	<-waitCh
}
