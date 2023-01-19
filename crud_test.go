package core

import (
	"encoding/binary"
	"errors"
	"github.com/couchbase/stellar-nebula/core/memdx"
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

func (f *fakeVBucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte, cb DispatchByKeyHandler) error {
	time.AfterFunc(f.delay, func() {
		cb(f.endpoint, 0, nil)
	})
	return nil
}

func (f *fakeVBucketDispatcher) DispatchToVbucket(ctx *AsyncContext, vbID uint16, cb DispatchToVbucketHandler) error {
	// TODO implement me. No, you can't make me.
	panic("implement me")
}

func (f *fakeVBucketDispatcher) StoreVbucketRoutingInfo(info *vbucketRoutingInfo) {
}

func (f *fakeVBucketDispatcher) Close() error {
	return nil
}

type fakeKvClient struct {
	onCall func(*memdx.Packet, memdx.DispatchCallback) error
}

func (cli *fakeKvClient) HasFeature(feat memdx.HelloFeature) bool {
	return true
}

func (cli *fakeKvClient) Dispatch(req *memdx.Packet, handler memdx.DispatchCallback) error {
	return cli.onCall(req, handler)
}

func (cli *fakeKvClient) Close() error {
	return nil
}

func (cli *fakeKvClient) LoadFactor() float64 {
	return 1.0
}

type fakeConnManager struct {
	cli KvClient
	err error
}

func (f *fakeConnManager) Execute(endpoint string, handler ConnExecuteHandler) error {
	return handler(f.cli, f.err)
}

func (f *fakeConnManager) UpdateEndpoints(serverList []string) error {
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
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		time.AfterFunc(0, func() {
			if called == 3 {
				handler(&memdx.Packet{}, nil)
				return
			}
			called++
			handler(nil, errors.New("rar"))
		})
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		connManager:   router,
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
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		time.AfterFunc(0, func() {
			if called == 3 {
				ctx.Cancel()
			}
			called++
			handler(nil, errors.New("rar"))
		})
		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		connManager:   router,
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
	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		time.AfterFunc(0, func() {
			called++
			if req.OpCode == memdx.OpCodeCollectionsGetID {
				pk := &memdx.Packet{
					Extras: make([]byte, 12),
				}
				binary.BigEndian.PutUint64(pk.Extras[0:], 4)
				binary.BigEndian.PutUint32(pk.Extras[8:], 9)
				handler(pk, nil)
				return
			} else if req.OpCode == memdx.OpCodeGet {
				pk := &memdx.Packet{
					Extras: make([]byte, 4),
					Value:  []byte("value"),
				}
				binary.BigEndian.PutUint32(pk.Extras[0:], 1)
				handler(pk, nil)
				return
			}

			handler(nil, errors.New("test should not have reached here"))
		})

		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	crud := &CrudComponent{
		collections: newCollectionResolver(router),
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		connManager:   router,
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

	cliCb := func(req *memdx.Packet, handler memdx.DispatchCallback) error {
		time.AfterFunc(0, func() {
			handler(&memdx.Packet{
				Status: 1,
			}, nil)
		})

		return nil
	}
	client := &fakeKvClient{
		onCall: cliCb,
	}
	router := &fakeConnManager{
		cli: client,
	}
	expectedErr := memdx.ErrDocNotFound
	crud := &CrudComponent{
		collections: &fakeCollectionResolver{
			cid: 7,
		},
		vbuckets: &fakeVBucketDispatcher{
			endpoint: "anendpoint",
		},
		connManager: router,
		retries:     &fakeRetryOrch{},
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
