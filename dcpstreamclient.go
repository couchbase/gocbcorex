package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type DcpStreamClient struct {
	cli               *DcpClient
	streamOpenHandler func(req *memdx.DcpStreamReqResponse)

	lock    sync.Mutex
	streams map[uint16]*dcpStreamClientVbucket
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
	BufferSize            int
	Priority              string
	ForceValueCompression bool
	EnableCursorDropping  bool
	EnableExpiryEvents    bool
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
		BufferSize:            opts.BufferSize,
		Priority:              opts.Priority,
		ForceValueCompression: opts.ForceValueCompression,
		EnableCursorDropping:  opts.EnableCursorDropping,
		EnableExpiryEvents:    opts.EnableExpiryEvents,
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

func (c *DcpStreamClient) OpenStream(ctx context.Context, opts *memdx.DcpStreamReqRequest) (DcpStream, error) {
	stream := &dcpStreamClientVbucket{
		parent: c,
		vbId:   opts.VbucketID,
	}

	c.lock.Lock()
	if c.streams == nil {
		c.streams = make(map[uint16]*dcpStreamClientVbucket)
	}

	_, hasStream := c.streams[opts.VbucketID]
	if hasStream {
		c.lock.Unlock()
		return nil, errors.New("stream already open")
	}

	c.streams[opts.VbucketID] = stream
	c.lock.Unlock()

	_, err := c.cli.DcpStreamReq(ctx, opts, func(resp *memdx.DcpStreamReqResponse) error {
		if c.streamOpenHandler != nil {
			c.streamOpenHandler(resp)
		}

		// we must not return an error here, as that will cause ambiguity on whether
		// or not the DcpStreamOpen event was emitted.
		return nil
	})
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (c *DcpStreamClient) Close() error {
	return c.cli.Close()
}

type dcpStreamClientVbucket struct {
	parent *DcpStreamClient
	vbId   uint16
}

var _ DcpStream = (*dcpStreamClientVbucket)(nil)

func (s *dcpStreamClientVbucket) Close(ctx context.Context) error {
	_, err := s.parent.cli.DcpCloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: s.vbId,
	})
	if err != nil {
		return err
	}

	s.parent.lock.Lock()
	delete(s.parent.streams, s.vbId)
	s.parent.lock.Unlock()

	return nil
}
