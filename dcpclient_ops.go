package gocbcorex

import (
	"context"
	"errors"

	"github.com/couchbase/gocbcorex/memdx"
)

func dcpClient_SimpleCall[Encoder any, ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *dcpClient,
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
		releaseSyncCrudResulter(resulter)

		if errors.Is(err, memdx.ErrDispatch) {
			err = KvClientDispatchError{err}
		}

		var emptyResp RespT
		return emptyResp, err
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

func (c *dcpClient) bootstrap(ctx context.Context, opts *memdx.BootstrapOptions) (*memdx.BootstrapResult, error) {
	return dcpClient_SimpleCall(ctx, c, memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}, memdx.OpBootstrap.Bootstrap, opts)
}
