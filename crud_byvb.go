package gocbcorex

import (
	"context"

	"github.com/couchbase/gocbcorex/memdx"
)

func OrchestrateSimpleVbCrud[RespT any](
	ctx context.Context,
	rs RetryManager,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	nkcp KvClientManager,
	vbID uint16, vbServerIdx uint32,
	fn func(endpoint string, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdRoutingByVbucketId(ctx, vb, ch, vbID, vbServerIdx,
				func(endpoint string) (RespT, error) {
					return OrchestrateMemdClient(ctx, nkcp, endpoint, func(client KvClient) (RespT, error) {
						return fn(endpoint, client)
					})
				})
		})
}

type StatsByVbucketOptions struct {
	GroupName  string
	VbucketID  uint16
	OnBehalfOf string
}

type StatsResult struct{}

type StatsDataResult struct {
	Key   string
	Value string
}

func (cc *CrudComponent) StatsByVbucket(
	ctx context.Context,
	opts *StatsByVbucketOptions,
	dataCb func(StatsDataResult),
) (*StatsResult, error) {
	ctx, span := tracer.Start(ctx, "Stats")
	defer span.End()

	return OrchestrateSimpleVbCrud(
		ctx, cc.retries, cc.vbs, cc.nmvHandler, cc.connManager,
		opts.VbucketID, 0,
		func(endpoint string, client KvClient) (*StatsResult, error) {
			_, err := client.Stats(ctx, &memdx.StatsRequest{
				GroupName: opts.GroupName,
				UtilsRequestMeta: memdx.UtilsRequestMeta{
					OnBehalfOf: opts.OnBehalfOf,
				},
			}, func(resp *memdx.StatsDataResponse) error {
				dataCb(StatsDataResult{
					Key:   resp.Key,
					Value: resp.Value,
				})
				return nil
			})
			if err != nil {
				return nil, err
			}

			return &StatsResult{}, nil
		})
}
