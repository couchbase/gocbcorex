package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type memdxPendingOpMock struct {
	cancelledCh chan error
}

func (mpo memdxPendingOpMock) Cancel(err error) {
	mpo.cancelledCh <- err
}

func TestKvClientReconfigureBucketOverExistingBucket(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc:   nil,
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		SelectedBucket: "imnotarealboy",
	}, func(error) {})
	require.Error(t, err)
}

func TestKvClientReconfigureTLSConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc:   nil,
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		TlsConfig: &tls.Config{},
	}, func(error) {})
	require.Error(t, err)
}

func TestKvClientReconfigureUsername(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc:   nil,
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user2",
			Password: "pass2",
		},
		TlsConfig: nil,
	}, func(error) {})
	require.Error(t, err)
}

func TestKvClientReconfigurePassword(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc:   nil,
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user2",
			Password: "pass2",
		},
		TlsConfig: nil,
	}, func(error) {})
	require.Error(t, err)
}

func TestKvClientReconfigureAddress(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc:   nil,
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address: "endpoint2",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		TlsConfig: nil,
	}, func(error) {})
	require.Error(t, err)
}

// This just tests that orphan responses are handled in some way.
func TestKvClientOrphanResponseHandler(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	cli.handleOrphanResponse(&memdx.Packet{OpCode: memdx.OpCodeSet, Opaque: 1})
}

func TestKvClientConnCloseHandlerDefault(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	cli.handleConnectionClose(errors.New("some error"))
	assert.Equal(t, 1, int(cli.closed))
}

func TestKvClientConnCloseHandlerCallsUpstream(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return memdxPendingOpMock{}, nil
		},
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	var closedCli KvClient
	var closeErr error
	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
		CloseHandler: func(client KvClient, err error) {
			closedCli = client
			closeErr = err
		},
	})
	require.NoError(t, err)

	err = errors.New("some error")
	cli.handleConnectionClose(err)
	assert.Equal(t, cli, closedCli)
	assert.Equal(t, err, closeErr)
}

func TestKvClientWrapsDispatchError(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return nil, memdx.ErrDispatch
		},
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	_, err = cli.Get(context.Background(), &memdx.GetRequest{})
	require.ErrorIs(t, err, memdx.ErrDispatch)

	var dispatchError KvClientDispatchError
	require.ErrorAs(t, err, &dispatchError)
}

func TestKvClientDoesNotWrapNonDispatchError(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return nil, memdx.ErrProtocol
		},
		RemoteAddrFunc: func() string { return "remote:1" },
		LocalAddrFunc:  func() string { return "local:2" },
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address: "endpoint1",

		// we set these to avoid bootstrapping
		DisableBootstrap:       true,
		DisableDefaultFeatures: true,
		DisableErrorMap:        true,
	}, &KvClientOptions{
		Logger: logger,
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	_, err = cli.Get(context.Background(), &memdx.GetRequest{})
	require.ErrorIs(t, err, memdx.ErrProtocol)

	var dispatchError KvClientDispatchError
	require.False(t, errors.As(err, &dispatchError), "error should not have dispatch error")
}
