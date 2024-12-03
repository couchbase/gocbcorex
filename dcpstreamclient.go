package gocbcorex

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type DcpStreamClient struct {
	cli               *DcpClient
	streamOpenHandler func(req *memdx.DcpStreamReqResponse)
}

type DcpStreamClientOptions struct {
	Logger                *zap.Logger
	Address               string
	TlsConfig             *tls.Config
	ClientName            string
	Authenticator         Authenticator
	Bucket                string
	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	NoopInterval          time.Duration
	Priority              string
	ForceValueCompression bool
	EnableExpiryEvents    bool
	EnableStreamIds       bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool
	Handlers              DcpEventsHandlers
}

func NewDcpStreamClient(
	ctx context.Context,
	opts *DcpStreamClientOptions,
) (*DcpStreamClient, error) {
	router := DcpStreamRouterStatic{
		ClientHandlers: opts.Handlers,
	}

	cli, err := NewDcpClient(ctx, &DcpClientOptions{
		Address:       opts.Address,
		TlsConfig:     opts.TlsConfig,
		ClientName:    opts.ClientName,
		Authenticator: opts.Authenticator,
		Bucket:        opts.Bucket,

		Handlers: router.Handlers(),

		ConnectionName:        opts.ConnectionName,
		ConsumerName:          opts.ConsumerName,
		ConnectionFlags:       opts.ConnectionFlags,
		NoopInterval:          opts.NoopInterval,
		Priority:              opts.Priority,
		ForceValueCompression: opts.ForceValueCompression,
		EnableExpiryEvents:    opts.EnableExpiryEvents,
		EnableStreamIds:       opts.EnableStreamIds,
		EnableOso:             opts.EnableOso,
		EnableSeqNoAdvance:    opts.EnableSeqNoAdvance,
		BackfillOrder:         opts.BackfillOrder,
		EnableChangeStreams:   opts.EnableChangeStreams,

		Logger:         opts.Logger,
		NewMemdxClient: nil,
		CloseHandler:   nil,
	})
	if err != nil {
		return nil, err
	}

	return &DcpStreamClient{
		cli:               cli,
		streamOpenHandler: opts.Handlers.StreamOpen,
	}, nil
}

type DcpStream interface {
	Close(ctx context.Context) error
}

func (c *DcpStreamClient) OpenStream(ctx context.Context, opts *memdx.DcpStreamReqRequest) error {
	_, err := c.cli.DcpStreamReq(ctx, opts, func(resp *memdx.DcpStreamReqResponse) error {
		if c.streamOpenHandler != nil {
			c.streamOpenHandler(resp)
		}

		// we must not return an error here, as that will cause ambiguity on whether
		// or not the DcpStreamOpen event was emitted.
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *DcpStreamClient) CloseStream(ctx context.Context, opts *memdx.DcpCloseStreamRequest) error {
	_, err := c.cli.DcpCloseStream(ctx, opts)
	return err
}

func (c *DcpStreamClient) Close() error {
	return c.cli.Close()
}
