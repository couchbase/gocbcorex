package core

import (
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

func (f *fakeVBucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte, cb func(string, error)) {
	time.AfterFunc(f.delay, func() {
		cb(f.endpoint, f.err)
	})
}

func (f *fakeVBucketDispatcher) DispatchToVbucket(ctx *AsyncContext, vbID uint16, cb func(string, error)) {
	// TODO implement me. No, you can't make me.
	panic("implement me")
}

type fakeServerDispatcher struct {
	onCall func(func(*memd.Packet, error))
}

func (f *fakeServerDispatcher) DispatchToServer(endpoint string, pak *memd.Packet, cb func(*memd.Packet, error)) {
	f.onCall(cb)
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
	routerCb := func(cb func(*memd.Packet, error)) {
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
	routerCb := func(cb func(*memd.Packet, error)) {
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
