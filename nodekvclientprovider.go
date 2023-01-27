package core

import (
	"context"
	"errors"
)

type NodeKvClientProvider interface {
	ShutdownClient(endpoint string, client KvClient)
	GetClient(ctx context.Context, endpoint string) (KvClient, error)
	Reconfigure(opts *KvClientManagerOptions) error
	GetRandomClient(ctx context.Context) (KvClient, error)
}

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
			var dispatchErr KvClientDispatchError
			if errors.As(err, &dispatchErr) {
				// this was a dispatch error, so we can just try with
				// a different client instead...
				cm.ShutdownClient(endpoint, cli)
				continue
			}

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
			var dispatchErr KvClientDispatchError
			if errors.As(err, &dispatchErr) {
				// this was a dispatch error, so we can just try with
				// a different client instead...
				cm.ShutdownClient("", cli)
				continue
			}

			return res, err
		}

		return res, nil
	}
}
