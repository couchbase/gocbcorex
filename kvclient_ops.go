package gocbcorex

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/memdx"
)

type KvClientDispatchError struct {
	Cause error
}

func (e KvClientDispatchError) Error() string {
	return fmt.Sprintf("dispatch error: %s", e.Cause)
}

func (e KvClientDispatchError) Unwrap() error {
	return e.Cause
}

type syncCrudResult struct {
	Result interface{}
	Err    error
}

type syncCrudResulter struct {
	Ch chan syncCrudResult
}

var syncCrudResulterPool sync.Pool

func allocSyncCrudResulter() *syncCrudResulter {
	resulter := syncCrudResulterPool.Get()
	if resulter == nil {
		return &syncCrudResulter{
			Ch: make(chan syncCrudResult, 1),
		}
	}
	return resulter.(*syncCrudResulter)
}
func releaseSyncCrudResulter(v *syncCrudResulter) {
	syncCrudResulterPool.Put(v)
}

func kvClient_SimpleCall[Encoder any, ReqT any, RespT any](
	ctx context.Context,
	c *kvClient,
	o Encoder,
	execFn func(o Encoder, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	resulter := allocSyncCrudResulter()
	var calledBack uint32

	pendingOp, err := execFn(o, c.cli, req, func(resp RespT, err error) {
		if atomic.CompareAndSwapUint32(&calledBack, 0, 1) {
			resulter.Ch <- syncCrudResult{
				Result: resp,
				Err:    err,
			}
		} else {
			c.logger.DPanic("callback invoked twice", zap.Any("resp", resp), zap.Error(err))
		}
	})
	if err != nil {
		if !atomic.CompareAndSwapUint32(&calledBack, 0, 1) {
			c.logger.DPanic("error returned after callback", zap.Error(err))
		}
		releaseSyncCrudResulter(resulter)
		var emptyResp RespT
		return emptyResp, KvClientDispatchError{err}
	}

	select {
	case res := <-resulter.Ch:
		releaseSyncCrudResulter(resulter)
		return res.Result.(RespT), res.Err
	case <-ctx.Done():
		pendingOp.Cancel(ctx.Err())

		res := <-resulter.Ch
		releaseSyncCrudResulter(resulter)
		return res.Result.(RespT), res.Err
	}
}

func kvClient_SimpleCoreCall[ReqT any, RespT any](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsCore, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpsCore{}, execFn, req)
}

func kvClient_SimpleUtilsCall[ReqT any, RespT any](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsUtils, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpsUtils{
		ExtFramesEnabled: c.HasFeature(memdx.HelloFeatureAltRequests),
	}, execFn, req)
}

func kvClient_SimpleCrudCall[ReqT any, RespT any](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsCrud, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpsCrud{
		ExtFramesEnabled:      c.HasFeature(memdx.HelloFeatureAltRequests),
		CollectionsEnabled:    c.HasFeature(memdx.HelloFeatureCollections),
		DurabilityEnabled:     c.HasFeature(memdx.HelloFeatureSyncReplication),
		PreserveExpiryEnabled: c.HasFeature(memdx.HelloFeaturePreserveExpiry),
	}, execFn, req)
}

func (c *kvClient) bootstrap(ctx context.Context, opts *memdx.BootstrapOptions) (*memdx.BootstrapResult, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}, memdx.OpBootstrap.Bootstrap, opts)
}

func (c *kvClient) GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) ([]byte, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.GetClusterConfig, req)
}

func selectBucketWrapper(o memdx.OpsCore, d memdx.Dispatcher, req *memdx.SelectBucketRequest, cb func([]byte, error)) (memdx.PendingOp, error) {
	return o.SelectBucket(d, req, func(err error) {
		cb(nil, err)
	})
}

func (c *kvClient) SelectBucket(ctx context.Context, req *memdx.SelectBucketRequest) error {
	_, err := kvClient_SimpleCoreCall(ctx, c, selectBucketWrapper, req)
	return err
}

func (c *kvClient) GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error) {
	return kvClient_SimpleUtilsCall(ctx, c, memdx.OpsUtils.GetCollectionID, req)
}

func (c *kvClient) Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Get, req)
}

func (c *kvClient) GetAndLock(ctx context.Context, req *memdx.GetAndLockRequest) (*memdx.GetAndLockResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetAndLock, req)
}

func (c *kvClient) GetAndTouch(ctx context.Context, req *memdx.GetAndTouchRequest) (*memdx.GetAndTouchResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetAndTouch, req)
}

func (c *kvClient) GetReplica(ctx context.Context, req *memdx.GetReplicaRequest) (*memdx.GetReplicaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetReplica, req)
}

func (c *kvClient) GetRandom(ctx context.Context, req *memdx.GetRandomRequest) (*memdx.GetRandomResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetRandom, req)
}

func (c *kvClient) Set(ctx context.Context, req *memdx.SetRequest) (*memdx.SetResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Set, req)
}

func (c *kvClient) Unlock(ctx context.Context, req *memdx.UnlockRequest) (*memdx.UnlockResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Unlock, req)
}

func (c *kvClient) Touch(ctx context.Context, req *memdx.TouchRequest) (*memdx.TouchResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Touch, req)
}

func (c *kvClient) Delete(ctx context.Context, req *memdx.DeleteRequest) (*memdx.DeleteResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Delete, req)
}

func (c *kvClient) Add(ctx context.Context, req *memdx.AddRequest) (*memdx.AddResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Add, req)
}

func (c *kvClient) Replace(ctx context.Context, req *memdx.ReplaceRequest) (*memdx.ReplaceResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Replace, req)
}

func (c *kvClient) Append(ctx context.Context, req *memdx.AppendRequest) (*memdx.AppendResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Append, req)
}

func (c *kvClient) Prepend(ctx context.Context, req *memdx.PrependRequest) (*memdx.PrependResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Prepend, req)
}

func (c *kvClient) Increment(ctx context.Context, req *memdx.IncrementRequest) (*memdx.IncrementResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Increment, req)
}

func (c *kvClient) Decrement(ctx context.Context, req *memdx.DecrementRequest) (*memdx.DecrementResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Decrement, req)
}

func (c *kvClient) GetMeta(ctx context.Context, req *memdx.GetMetaRequest) (*memdx.GetMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetMeta, req)
}

func (c *kvClient) SetMeta(ctx context.Context, req *memdx.SetMetaRequest) (*memdx.SetMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.SetMeta, req)
}

func (c *kvClient) DeleteMeta(ctx context.Context, req *memdx.DeleteMetaRequest) (*memdx.DeleteMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.DeleteMeta, req)
}

func (c *kvClient) LookupIn(ctx context.Context, req *memdx.LookupInRequest) (*memdx.LookupInResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.LookupIn, req)
}

func (c *kvClient) MutateIn(ctx context.Context, req *memdx.MutateInRequest) (*memdx.MutateInResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.MutateIn, req)
}
