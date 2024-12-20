package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/memdx"
)

var (
	meter = otel.Meter("github.com/couchbase/gocbcorex")
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
	Ch         chan syncCrudResult
	AllocCount uint32
	SendCount  uint32
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

func kvClient_SimpleCall[Encoder any, ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	o Encoder,
	execFn func(o Encoder, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	bucketName := c.SelectedBucket()
	localHost, localPort := hostPortFromNetAddr(c.LocalAddr())
	remoteHost, remotePort := hostPortFromNetAddr(c.RemoteAddr())

	stime := time.Now()

	ctx, span := tracer.Start(ctx, "memcached/"+req.OpName(),
		trace.WithSpanKind(trace.SpanKindClient))
	if span.IsRecording() {
		span.SetAttributes(
			semconv.ServerAddress(remoteHost),
			semconv.ServerPort(remotePort),
			semconv.NetworkPeerAddress(localHost),
			semconv.NetworkPeerPort(localPort),
			semconv.RPCMethod(req.OpName()),
			semconv.RPCSystemKey.String("memcached"))
	}

	finishCall := func(err error) {
		etime := time.Now()
		dtime := etime.Sub(stime)
		dtimeSecs := float64(dtime) / float64(time.Second)

		if err != nil {
			span.RecordError(err)
		}
		span.End()

		c.durationMetric.Record(ctx, dtimeSecs,
			metric.WithAttributes(
				semconv.DBSystemCouchbase,
				semconv.ServerAddress(remoteHost),
				semconv.ServerPort(remotePort),
				semconv.NetworkPeerAddress(localHost),
				semconv.NetworkPeerPort(localPort),
				semconv.DBNamespace(bucketName),
				semconv.DBOperationName(req.OpName()),
			),
		)

	}

	resulter := allocSyncCrudResulter()
	atomic.AddUint32(&resulter.AllocCount, 1)

	pendingOp, err := execFn(o, c.cli, req, func(resp RespT, err error) {
		if err != nil && bucketName != "" {
			err = &KvBucketError{
				Cause:      err,
				BucketName: bucketName,
			}
		}

		span.AddEvent("RECEIVED")

		if span.IsRecording() {
			var emptyResp RespT
			if resp != emptyResp {
				if sdResp, _ := any(resp).(memdx.ServerDurationResponse); sdResp != nil {
					span.SetAttributes(attribute.Int(
						"db.couchbase.server_duration",
						int(sdResp.GetServerDuration()/time.Microsecond)))
				}
			}
		}

		newSendCount := atomic.AddUint32(&resulter.SendCount, 1)
		if newSendCount != atomic.LoadUint32(&resulter.AllocCount) {
			c.logger.DPanic("sync resulter executed multiple sends", zap.Any("resp", resp), zap.Error(err))
		}

		resulter.Ch <- syncCrudResult{
			Result: resp,
			Err:    err,
		}
	})
	if err != nil {
		newSendCount := atomic.AddUint32(&resulter.SendCount, 1)
		if newSendCount != atomic.LoadUint32(&resulter.AllocCount) {
			c.logger.DPanic("sync resulter executed error after send", zap.Error(err))
		}

		releaseSyncCrudResulter(resulter)
		finishCall(err)

		if errors.Is(err, memdx.ErrDispatch) {
			err = &KvClientDispatchError{err}
		}

		var emptyResp RespT
		return emptyResp, err
	}

	span.AddEvent("SENT")

	select {
	case res := <-resulter.Ch:
		releaseSyncCrudResulter(resulter)
		finishCall(err)

		return res.Result.(RespT), res.Err
	case <-ctx.Done():
		pendingOp.Cancel(ctx.Err())
		res := <-resulter.Ch

		releaseSyncCrudResulter(resulter)
		finishCall(ctx.Err())

		return res.Result.(RespT), res.Err
	}
}

func kvClient_SimpleCoreCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsCore, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpsCore{}, execFn, req)
}

func kvClient_SimpleUtilsCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *kvClient,
	execFn func(o memdx.OpsUtils, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return kvClient_SimpleCall(ctx, c, memdx.OpsUtils{
		ExtFramesEnabled: c.HasFeature(memdx.HelloFeatureAltRequests),
	}, execFn, req)
}

func kvClient_SimpleCrudCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
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

func (c *kvClient) GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) (*memdx.GetClusterConfigResponse, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.GetClusterConfig, req)
}

func (c *kvClient) SelectBucket(ctx context.Context, req *memdx.SelectBucketRequest) (*memdx.SelectBucketResponse, error) {
	return kvClient_SimpleCoreCall(ctx, c, memdx.OpsCore.SelectBucket, req)
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
