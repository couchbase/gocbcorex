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
	reconfigureCh := make(chan struct{})
	err = cli.Reconfigure(&KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		Authenticator:  auth,
		SelectedBucket: testutils.TestOpts.BucketName,
	}, func(err error) {
		require.NoError(t, err)
		reconfigureCh <- struct{}{}
	})
	require.NoError(t, err)
	<-reconfigureCh

	// Check that an op works
	setRes, err := cli.Set(context.Background(), &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	// We don't know if we sent the Set to the correct node for the vbucket so check that the result is either ok or
	// is a NMVB error.
	if err == nil {
		assert.NotZero(t, setRes.Cas)
	} else {
		assert.ErrorIs(t, err, memdx.ErrNotMyVbucket)
	}

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

func TestKvClientCloseAfterReconfigure(t *testing.T) {
	testutils.SkipIfShortTest(t)

	auth := &PasswordAuthenticator{
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	}
	clientConfig := &KvClientConfig{
		Address:       testutils.TestOpts.MemdAddrs[0],
		TlsConfig:     nil,
		Authenticator: auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	cli, err := NewKvClient(context.Background(), clientConfig, &KvClientOptions{Logger: logger})
	require.NoError(t, err)

	err = cli.Reconfigure(&KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		TlsConfig:      nil,
		SelectedBucket: testutils.TestOpts.BucketName,
		Authenticator:  auth,
	}, func(err error) {
		// We don't need to wait for all of the clients to be fully reconfigured.
	})
	require.NoError(t, err)

	err = cli.Close()
	require.NoError(t, err)
}
