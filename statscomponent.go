package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
)

type StatsComponent struct {
	nmvHandler  NotMyVbucketConfigHandler
	retries     RetryManager
	connManager KvClientManager
	vbs         VbucketRouter
}

func (cc *StatsComponent) VbucketDetails(
	ctx context.Context,
	opts *memdx.StatsVbucketDetailsRequest,
) (*memdx.StatsVbucketDetailsResponse, error) {
	ctx, span := tracer.Start(ctx, "VbucketDetails")
	defer span.End()

	return OrchestrateRetries(
		ctx, cc.retries,
		func() (*memdx.StatsVbucketDetailsResponse, error) {
			return OrchestrateMemdRoutingByVbucketId(
				ctx, cc.vbs, cc.nmvHandler, opts.VbucketID, 0,
				func(endpoint string) (*memdx.StatsVbucketDetailsResponse, error) {
					return OrchestrateMemdClient(
						ctx, cc.connManager, endpoint,
						func(client KvClient) (*memdx.StatsVbucketDetailsResponse, error) {
							return client.StatsVbucketDetails(ctx, opts)
						})
				})
		})
}
