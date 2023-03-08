package gocbcorex

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
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

func TestKvClientReconfigureBucket(t *testing.T) {
	testutils.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	auth := &PasswordAuthenticator{
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Address:       testutils.TestOpts.MemdAddrs[0],
		Authenticator: auth,
	}, &KvClientOptions{
		Logger: logger,
	})
	require.NoError(t, err)

	// Select a bucket on a gcccp level request
	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		Authenticator:  auth,
		SelectedBucket: testutils.TestOpts.BucketName,
	})
	require.NoError(t, err)

	// Check that an op works
	setRes, err := cli.Set(context.Background(), &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	require.NoError(t, err)
	assert.NotZero(t, setRes.Cas)

	err = cli.Close()
	require.NoError(t, err)
}

func TestKvClientReconfigureBucketOverExistingBucket(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
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

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		SelectedBucket: "imnotarealboy",
	})
	require.Error(t, err)
}

func TestKvClientReconfigureTLSConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
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

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		TlsConfig: &tls.Config{},
	})
	require.Error(t, err)
}

func TestKvClientReconfigureUsername(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
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

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user2",
			Password: "pass2",
		},
		TlsConfig: nil,
	})
	require.Error(t, err)
}

func TestKvClientReconfigurePassword(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
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

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user2",
			Password: "pass2",
		},
		TlsConfig: nil,
	})
	require.Error(t, err)
}

func TestKvClientReconfigureAddress(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
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

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address: "endpoint2",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		TlsConfig: nil,
	})
	require.Error(t, err)
}
