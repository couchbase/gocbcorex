package core

import (
	"context"
	"sync"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

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

	pendingOp, err := execFn(o, c.cli, req, func(resp RespT, err error) {
		resulter.Ch <- syncCrudResult{
			Result: resp,
			Err:    err,
		}
	})
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	select {
	case res := <-resulter.Ch:
		releaseSyncCrudResulter(resulter)
		return res.Result.(RespT), res.Err
	case <-ctx.Done():
		if !pendingOp.Cancel() {
			<-resulter.Ch
		}
		releaseSyncCrudResulter(resulter)
		var emptyResp RespT
		return emptyResp, ctx.Err()
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
		ExtFramesEnabled:   c.HasFeature(memdx.HelloFeatureAltRequests),
		CollectionsEnabled: c.HasFeature(memdx.HelloFeatureCollections),
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

func (c *kvClient) GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error) {
	return kvClient_SimpleUtilsCall(ctx, c, memdx.OpsUtils.GetCollectionID, req)
}

func (c *kvClient) Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Get, req)
}
