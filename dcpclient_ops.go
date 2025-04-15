package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
)

func dcpClient_SimpleDcpCall[ReqT memdx.OpRequest, RespT memdx.OpResponse](
	ctx context.Context,
	c *DcpClient,
	execFn func(o memdx.OpsDcp, d memdx.Dispatcher, req ReqT, cb func(RespT, error)) (memdx.PendingOp, error),
	req ReqT,
) (RespT, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpsDcp{
		ExtFramesEnabled:   c.HasFeature(memdx.HelloFeatureAltRequests),
		CollectionsEnabled: c.HasFeature(memdx.HelloFeatureCollections),
		StreamIdsEnabled:   false,
	}, execFn, req)
}

func (c *DcpClient) bootstrap(ctx context.Context, opts *memdx.BootstrapOptions) (*memdx.BootstrapResult, error) {
	return memdClient_SimpleCall(ctx, c, memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}, memdx.OpBootstrap.Bootstrap, opts)
}

func (c *DcpClient) dcpOpenConnection(ctx context.Context, req *memdx.DcpOpenConnectionRequest) (*memdx.DcpOpenConnectionResponse, error) {
	return dcpClient_SimpleDcpCall(ctx, c, memdx.OpsDcp.DcpOpenConnection, req)
}

func (c *DcpClient) dcpControl(ctx context.Context, req *memdx.DcpControlRequest) (*memdx.DcpControlResponse, error) {
	return dcpClient_SimpleDcpCall(ctx, c, memdx.OpsDcp.DcpControl, req)
}

func (c *DcpClient) DcpStreamReq(ctx context.Context,
	req *memdx.DcpStreamReqRequest,
	syncCb func(*memdx.DcpStreamReqResponse),
) (*memdx.DcpStreamReqResponse, error) {
	return dcpClient_SimpleDcpCall(ctx, c,
		func(o memdx.OpsDcp,
			d memdx.Dispatcher,
			req *memdx.DcpStreamReqRequest,
			cb func(*memdx.DcpStreamReqResponse, error),
		) (memdx.PendingOp, error) {
			return memdx.OpsDcp.DcpStreamReq(o, d, req, func(resp *memdx.DcpStreamReqResponse, err error) {
				if err == nil {
					if syncCb != nil {
						syncCb(resp)
					}
				}
				cb(resp, err)
			})
		}, req)
}

func (c *DcpClient) DcpCloseStream(ctx context.Context, req *memdx.DcpCloseStreamRequest) (*memdx.DcpCloseStreamResponse, error) {
	return dcpClient_SimpleDcpCall(ctx, c, memdx.OpsDcp.DcpCloseStream, req)
}

type StatsResponse struct {
	Values map[string]string
}

func (c *DcpClient) Stats(ctx context.Context, req *memdx.StatsRequest) (*StatsResponse, error) {
	allStats := make(map[string]string)
	return memdClient_SimpleCall(ctx, c, memdx.OpsUtils{
		ExtFramesEnabled: c.HasFeature(memdx.HelloFeatureAltRequests),
	}, func(
		o memdx.OpsUtils,
		d memdx.Dispatcher,
		req *memdx.StatsRequest,
		cb func(*StatsResponse, error),
	) (memdx.PendingOp, error) {
		return memdx.OpsUtils.Stats(o, d, req, func(resp *memdx.StatsDataResponse) {
			allStats[resp.Key] = resp.Value
		}, func(resp *memdx.StatsActionResponse, err error) {
			if err != nil {
				cb(nil, err)
				return
			}

			cb(&StatsResponse{
				Values: allStats,
			}, nil)
		})
	}, req)
}
