package gocbcorex

import "context"

func OrchestrateSimpleVbCrud[RespT any](
	ctx context.Context,
	rs RetryManager,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	ecp KvEndpointClientProvider,
	vbID uint16, vbServerIdx uint32,
	fn func(endpoint string, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdRoutingByVbucketId(ctx, vb, ch, vbID, vbServerIdx,
				func(endpoint string) (RespT, error) {
					return OrchestrateEndpointKvClient(ctx, ecp, endpoint, func(client KvClient) (RespT, error) {
						return fn(endpoint, client)
					})
				})
		})
}
