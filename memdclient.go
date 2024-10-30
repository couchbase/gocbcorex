package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
)

type MemdClientDispatchError struct {
	Cause error
}

func (e MemdClientDispatchError) Error() string {
	return fmt.Sprintf("dispatch error: %s", e.Cause)
}

func (e MemdClientDispatchError) Unwrap() error {
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

type MemdClientTelem interface {
	BeginOp(ctx context.Context, bucketName string, opName string) (context.Context, MemdClientTelemOp)
}

type MemdClientTelemOp interface {
	IsRecording() bool
	MarkSent()
	MarkReceived()
	RecordServerDuration(d time.Duration)
	End(ctx context.Context, err error)
}

type MemdClient interface {
	SelectedBucket() string
	Telemetry() MemdClientTelem
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
	ctx, opTelem := c.Telemetry().BeginOp(ctx, bucketName, req.OpName())

	resulter := allocSyncCrudResulter()

	pendingOp, err := execFn(o, c, req, func(resp RespT, err error) {
		if err != nil && bucketName != "" {
			err = &KvBucketError{
				Cause:      err,
				BucketName: bucketName,
			}
		}

		opTelem.MarkReceived()

		if opTelem.IsRecording() {
			var emptyResp RespT
			if resp != emptyResp {
				if sdResp, _ := any(resp).(memdx.ServerDurationResponse); sdResp != nil {
					opTelem.RecordServerDuration(sdResp.GetServerDuration())
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
		opTelem.End(ctx, err)

		if errors.Is(err, memdx.ErrDispatch) {
			err = &MemdClientDispatchError{err}
		}

		var emptyResp RespT
		return emptyResp, err
	}

	opTelem.MarkSent()

	select {
	case res := <-resulter.Ch:
		releaseSyncCrudResulter(resulter)
		opTelem.End(ctx, err)

		return res.Result.(RespT), res.Err
	case <-ctx.Done():
		pendingOp.Cancel(ctx.Err())
		res := <-resulter.Ch

		releaseSyncCrudResulter(resulter)
		opTelem.End(ctx, ctx.Err())

		return res.Result.(RespT), res.Err
	}
}
