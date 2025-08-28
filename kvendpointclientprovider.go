package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"net"
)

var (
	ErrInvalidEndpoint = errors.New("invalid endpoint")
)

type InvalidEndpointError struct {
	Endpoing string
}

func (e InvalidEndpointError) Error() string {
	return "invalid endpoint: " + e.Endpoing
}

func (e InvalidEndpointError) Unwrap() error {
	return ErrInvalidEndpoint
}

type KvEndpointClientProvider interface {
	KvClientProvider
	GetEndpointClient(ctx context.Context, endpoint string) (KvClient, error)
}

type KvClientError struct {
	Cause          error
	RemoteHostname string
	RemoteAddr     net.Addr
	LocalAddr      net.Addr
}

func (e *KvClientError) Error() string {
	return fmt.Sprintf("kv client error: %s (remote-host: %s, remote-addr: %s, local-addr: %s)",
		e.Cause, e.RemoteHostname, e.RemoteAddr, e.LocalAddr)
}

func (e *KvClientError) Unwrap() error {
	return e.Cause
}

func OrchestrateEndpointKvClient[RespT any](
	ctx context.Context,
	cm KvEndpointClientProvider,
	endpoint string,
	fn func(client KvClient) (RespT, error),
) (RespT, error) {
	for {
		cli, err := cm.GetEndpointClient(ctx, endpoint)
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
