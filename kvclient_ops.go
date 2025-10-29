package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
)

type KvClientOps interface {
	GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error)
	GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) (*memdx.GetClusterConfigResponse, error)
	Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error)
	GetEx(ctx context.Context, req *memdx.GetExRequest) (*memdx.GetExResponse, error)
	Set(ctx context.Context, req *memdx.SetRequest) (*memdx.SetResponse, error)
	Delete(ctx context.Context, req *memdx.DeleteRequest) (*memdx.DeleteResponse, error)
	GetAndLock(ctx context.Context, req *memdx.GetAndLockRequest) (*memdx.GetAndLockResponse, error)
	GetAndTouch(ctx context.Context, req *memdx.GetAndTouchRequest) (*memdx.GetAndTouchResponse, error)
	GetReplica(ctx context.Context, req *memdx.GetReplicaRequest) (*memdx.GetReplicaResponse, error)
	GetRandom(ctx context.Context, req *memdx.GetRandomRequest) (*memdx.GetRandomResponse, error)
	Unlock(ctx context.Context, req *memdx.UnlockRequest) (*memdx.UnlockResponse, error)
	Touch(ctx context.Context, req *memdx.TouchRequest) (*memdx.TouchResponse, error)
	Add(ctx context.Context, req *memdx.AddRequest) (*memdx.AddResponse, error)
	Replace(ctx context.Context, req *memdx.ReplaceRequest) (*memdx.ReplaceResponse, error)
	Append(ctx context.Context, req *memdx.AppendRequest) (*memdx.AppendResponse, error)
	Prepend(ctx context.Context, req *memdx.PrependRequest) (*memdx.PrependResponse, error)
	Increment(ctx context.Context, req *memdx.IncrementRequest) (*memdx.IncrementResponse, error)
	Decrement(ctx context.Context, req *memdx.DecrementRequest) (*memdx.DecrementResponse, error)
	GetMeta(ctx context.Context, req *memdx.GetMetaRequest) (*memdx.GetMetaResponse, error)
	AddWithMeta(ctx context.Context, req *memdx.AddWithMetaRequest) (*memdx.AddWithMetaResponse, error)
	SetWithMeta(ctx context.Context, req *memdx.SetWithMetaRequest) (*memdx.SetWithMetaResponse, error)
	DeleteWithMeta(ctx context.Context, req *memdx.DeleteWithMetaRequest) (*memdx.DeleteWithMetaResponse, error)
	LookupIn(ctx context.Context, req *memdx.LookupInRequest) (*memdx.LookupInResponse, error)
	MutateIn(ctx context.Context, req *memdx.MutateInRequest) (*memdx.MutateInResponse, error)
	RangeScanCreate(ctx context.Context, req *memdx.RangeScanCreateRequest) (*memdx.RangeScanCreateResponse, error)
	RangeScanContinue(ctx context.Context, req *memdx.RangeScanContinueRequest,
		dataCb func(*memdx.RangeScanDataResponse) error) (*memdx.RangeScanActionResponse, error)
	RangeScanCancel(ctx context.Context, req *memdx.RangeScanCancelRequest) (*memdx.RangeScanCancelResponse, error)
	Stats(ctx context.Context, req *memdx.StatsRequest, dataCb func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error)
}

func kvClient_SimpleCoreCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsCore, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpsCore{}, execFn, req)
}

func kvClient_SimpleUtilsCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsUtils, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpsUtils{
		ExtFramesEnabled: c.HasFeature(memdx.HelloFeatureAltRequests),
	}, execFn, req)
}

func kvClient_SimpleCrudCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsCrud, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpsCrud{
		ExtFramesEnabled:      c.HasFeature(memdx.HelloFeatureAltRequests),
		CollectionsEnabled:    c.HasFeature(memdx.HelloFeatureCollections),
		DurabilityEnabled:     c.HasFeature(memdx.HelloFeatureSyncReplication),
		PreserveExpiryEnabled: c.HasFeature(memdx.HelloFeaturePreserveExpiry),
	}, execFn, req)
}

func (c *kvClient) bootstrap(ctx context.Context, opts *memdx.BootstrapOptions) (*memdx.BootstrapResult, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}, memdx.OpBootstrap.Bootstrap, opts)
}

func (c *kvClient) selectBucket(ctx context.Context, req *memdx.SelectBucketRequest) (*memdx.SelectBucketResponse, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.SelectBucket, req)
}

func (c *kvClient) GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) (*memdx.GetClusterConfigResponse, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.GetClusterConfig, req)
}

func (c *kvClient) NoOp(ctx context.Context, req *memdx.NoOpRequest) (*memdx.NoOpResponse, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.NoOp, req)
}

func (c *kvClient) GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error) {
	return kvClient_SimpleUtilsCall(ctx, c, memdx.OpsUtils.GetCollectionID, req)
}

func (c *kvClient) Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.Get, req)
}

func (c *kvClient) GetEx(ctx context.Context, req *memdx.GetExRequest) (*memdx.GetExResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.GetEx, req)
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

func (c *kvClient) AddWithMeta(ctx context.Context, req *memdx.AddWithMetaRequest) (*memdx.AddWithMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.AddWithMeta, req)
}

func (c *kvClient) SetWithMeta(ctx context.Context, req *memdx.SetWithMetaRequest) (*memdx.SetWithMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.SetWithMeta, req)
}

func (c *kvClient) DeleteWithMeta(ctx context.Context, req *memdx.DeleteWithMetaRequest) (*memdx.DeleteWithMetaResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.DeleteWithMeta, req)
}

func (c *kvClient) LookupIn(ctx context.Context, req *memdx.LookupInRequest) (*memdx.LookupInResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.LookupIn, req)
}

func (c *kvClient) MutateIn(ctx context.Context, req *memdx.MutateInRequest) (*memdx.MutateInResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.MutateIn, req)
}

func (c *kvClient) RangeScanCreate(ctx context.Context, req *memdx.RangeScanCreateRequest) (*memdx.RangeScanCreateResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.RangeScanCreate, req)
}

func (c *kvClient) RangeScanContinue(
	ctx context.Context,
	req *memdx.RangeScanContinueRequest,
	dataCb func(*memdx.RangeScanDataResponse) error,
) (*memdx.RangeScanActionResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c,
		func(o memdx.OpsCrud,
			d memdx.Dispatcher,
			req *memdx.RangeScanContinueRequest,
			cb func(*memdx.RangeScanActionResponse, error),
		) (memdx.PendingOp, error) {
			return o.RangeScanContinue(d, req, dataCb, cb)
		}, req)
}

func (c *kvClient) RangeScanCancel(ctx context.Context, req *memdx.RangeScanCancelRequest) (*memdx.RangeScanCancelResponse, error) {
	return kvClient_SimpleCrudCall(ctx, c, memdx.OpsCrud.RangeScanCancel, req)
}

func (c *kvClient) Stats(
	ctx context.Context,
	req *memdx.StatsRequest,
	dataCb func(*memdx.StatsDataResponse) error,
) (*memdx.StatsActionResponse, error) {
	return kvClient_SimpleUtilsCall(ctx, c,
		func(o memdx.OpsUtils,
			d memdx.Dispatcher,
			req *memdx.StatsRequest,
			cb func(*memdx.StatsActionResponse, error),
		) (memdx.PendingOp, error) {
			return o.Stats(d, req, dataCb, cb)
		}, req)
}
