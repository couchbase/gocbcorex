package gocbcorex

import (
	"context"
	"errors"
)

var (
	ErrClientStillConnecting = contextualDeadline{"still waiting for a connection to be established"}
)

type KvClientProvider interface {
	GetClient(ctx context.Context) (KvClient, error)
}

func OrchestrateKvClient[RespT any](
	ctx context.Context,
	cm KvClientProvider,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetClient(ctx)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(cli)
		if err != nil {
			var dispatchErr *KvDispatchNetError
			if errors.As(err, &dispatchErr) {
				// this was a dispatch error, so we can just try with
				// a different client instead...
				continue
			}

			return res, &KvClientError{
				Cause:          err,
				RemoteHostname: cli.RemoteHostname(),
				RemoteAddr:     cli.RemoteAddr(),
				LocalAddr:      cli.LocalAddr(),
			}
		}

		return res, nil
	}
}
