package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
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

type MemdClient interface {
	SelectedBucket() string
	MemdxClient
}

func memdClient_SimpleCall[Encoder any, ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c MemdClient,
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

		clientOpDurationMetric.Record(ctx, dtimeSecs,
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

	pendingOp, err := execFn(o, c, req, func(resp RespT, err error) {
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

		resulter.Ch <- syncCrudResult{
			Result: resp,
			Err:    err,
		}
	})
	if err != nil {
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
