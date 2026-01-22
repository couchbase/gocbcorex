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
	Endpoint string
}

func (e InvalidEndpointError) Error() string {
	return "invalid endpoint: " + e.Endpoint
}

func (e InvalidEndpointError) Unwrap() error {
	return ErrInvalidEndpoint
}

type KvEndpointClientProvider interface {
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

type EndpointKvClientOrchestrator struct {
	ctx      context.Context
	cm       KvEndpointClientProvider
	endpoint string

	resolvedClient KvClient
}

func NewEndpointKvClientOrchestrator(
	ctx context.Context,
	cm KvEndpointClientProvider,
	endpoint string,
) (*EndpointKvClientOrchestrator, error) {
	o := &EndpointKvClientOrchestrator{
		ctx:      ctx,
		cm:       cm,
		endpoint: endpoint,
	}

	err := o.begin()
	if err != nil {
		return nil, err
	}

	return o, nil
}

func (o *EndpointKvClientOrchestrator) begin() error {
	cli, err := o.cm.GetEndpointClient(o.ctx, o.endpoint)
	if err != nil {
		return err
	}

	o.resolvedClient = cli
	return nil
}

func (o *EndpointKvClientOrchestrator) HandleError(err error) error {
	var dispatchErr *KvDispatchNetError
	if errors.As(err, &dispatchErr) {
		// this was a dispatch error, so we can just try with
		// a different client instead...

		cli, err := o.cm.GetEndpointClient(o.ctx, o.endpoint)
		if err != nil {
			return err
		}

		o.resolvedClient = cli

		return nil
	}

	return &KvClientError{
		Cause:          err,
		RemoteHostname: o.resolvedClient.RemoteHostname(),
		RemoteAddr:     o.resolvedClient.RemoteAddr(),
		LocalAddr:      o.resolvedClient.LocalAddr(),
	}
}

func (o *EndpointKvClientOrchestrator) GetClient() KvClient {
	return o.resolvedClient
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
