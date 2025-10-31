package gocbcorex

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func singleClientDialFunc(client MemdxClient) DialMemdxClientFunc {
	return func(
		ctx context.Context,
		address string,
		dialOpts *memdx.DialConnOptions,
		clientOpts *memdx.ClientOptions,
	) (MemdxClient, error) {
		return client, nil
	}
}

type memdxPendingOpMock struct {
	cancelledCh chan error
}

func (mpo memdxPendingOpMock) Cancel(err error) bool {
	mpo.cancelledCh <- err
	return true
}

// This just tests that orphan responses are handled in some way.
func TestKvClientOrphanResponseHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxClientMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
	}

	cli, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:          logger,
		DialMemdxClient: singleClientDialFunc(memdxCli),

		Address: "endpoint1",

		// we set these to avoid bootstrapping
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
	require.NoError(t, err)

	cli.(*kvClient).handleOrphanResponse(&memdx.Packet{OpCode: memdx.OpCodeSet, Opaque: 1})
}

func TestKvClientConnCloseHandlerDefault(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxClientMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
		CloseFunc:      func() error { return nil },
	}

	cli, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:          logger,
		DialMemdxClient: singleClientDialFunc(memdxCli),

		Address: "endpoint1",

		// we set these to avoid bootstrapping
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
	require.NoError(t, err)

	kvCli := cli.(*kvClient)

	kvCli.handleConnectionReadError(errors.New("some error"))
	assert.Equal(t, true, kvCli.closed.Load())
}

func TestKvClientConnCloseHandlerCallsUpstream(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxClientMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
		CloseFunc:      func() error { return nil },
	}

	var closedCli KvClient
	var closeErr error
	cli, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:          logger,
		DialMemdxClient: singleClientDialFunc(memdxCli),
		CloseHandler: func(client KvClient, err error) {
			closedCli = client
			closeErr = err
		},

		Address: "endpoint1",

		// we set these to avoid bootstrapping
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
	require.NoError(t, err)

	err = errors.New("some error")
	cli.(*kvClient).handleConnectionReadError(err)
	assert.Equal(t, closedCli, cli)
	assert.Equal(t, closeErr, err)
}

func TestKvClientWrapsDispatchError(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxClientMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return nil, &net.OpError{
				Op:   "write",
				Net:  "tcp",
				Addr: nil,
				Err:  net.ErrClosed,
			}
		},
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
		CloseFunc:      func() error { return nil },
	}

	cli, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:          logger,
		DialMemdxClient: singleClientDialFunc(memdxCli),
		Address:         "endpoint1",

		// we set these to avoid bootstrapping
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
	require.NoError(t, err)

	_, err = cli.Get(context.Background(), &memdx.GetRequest{})
	require.Error(t, err)

	var dispatchErr *KvDispatchNetError
	require.ErrorAs(t, err, &dispatchErr)
}

func TestKvClientDoesNotWrapNonDispatchError(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxClientMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return nil, memdx.ErrProtocol
		},
		LocalAddrFunc:  func() net.Addr { return &net.TCPAddr{} },
		RemoteAddrFunc: func() net.Addr { return &net.TCPAddr{} },
		CloseFunc:      func() error { return nil },
	}

	cli, err := NewKvClient(context.Background(), &KvClientOptions{
		Logger:          logger,
		DialMemdxClient: singleClientDialFunc(memdxCli),

		Address: "endpoint1",

		// we set these to avoid bootstrapping
		BootstrapOpts: KvClientBootstrapOptions{
			DisableBootstrap:       true,
			DisableDefaultFeatures: true,
			DisableErrorMap:        true,
		},
	})
	require.NoError(t, err)

	_, err = cli.Get(context.Background(), &memdx.GetRequest{})
	require.ErrorIs(t, err, memdx.ErrProtocol)

	var dispatchErr *KvDispatchNetError
	require.NotErrorAs(t, err, &dispatchErr)
}
