package core

import (
	"context"
)

func OrchestrateMemdClient[RespT any](
	ctx context.Context,
	cm NodeKvClientProvider,
	endpoint string,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetClient(ctx, endpoint)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			return res, err
		}

		return res, nil
	}
}
func OrchestrateRandomMemdClient[RespT any](
	ctx context.Context,
	cm NodeKvClientProvider,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetRandomClient(ctx)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			return res, err
		}

		return res, nil
	}
}

func OrchestrateSimpleCrud[RespT any](
	ctx context.Context,
	rs RetryManager,
	cr CollectionResolver,
	cm ConfigManager,
	nkcp NodeKvClientProvider,
	scopeName, collectionName string,
	key []byte,
	fn func(collectionID uint32, manifestID uint64, endpoint string, vbID uint16, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateMemdRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdCollectionID(
				ctx, cr, scopeName, collectionName,
				func(collectionID uint32, manifestID uint64) (RespT, error) {
					return OrchestrateConfig(
						ctx, cm, key,
						func(endpoint string, vbID uint16) (RespT, error) {
							return OrchestrateMemdClient(ctx, nkcp, endpoint, func(client KvClient) (RespT, error) {
								return fn(collectionID, manifestID, endpoint, vbID, client)
							})
						})

				})
		})
}
