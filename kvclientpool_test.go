package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvClientPoolGetClient(t *testing.T) {
	mock := makeMockKvClient()
	clientConfig := KvClientConfig{
		Address:        "endpoint1",
		TlsConfig:      nil,
		SelectedBucket: "test",
		Authenticator: &PasswordAuthenticator{
			Username: "username",
			Password: "password",
		},
	}
	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 1,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &clientConfig, config)

			return mock, nil
		},
	})
	require.NoError(t, err)

	// This is done twice to test different codepaths, this is maybe testing implementation detail a bit.
	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	cli, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	assert.NoError(t, pool.Close())
}

func TestKvClientPoolGetClientConcurrent(t *testing.T) {
	mock := makeMockKvClient()
	clientConfig := KvClientConfig{
		Address:        "endpoint1",
		TlsConfig:      nil,
		SelectedBucket: "test",
		Authenticator: &PasswordAuthenticator{
			Username: "username",
			Password: "password",
		},
	}
	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 1,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &clientConfig, config)

			return mock, nil
		},
	})
	require.NoError(t, err)

	var wait sync.WaitGroup
	for i := 0; i < 50; i++ {
		wait.Add(1)
		go func() {
			cli, err := pool.GetClient(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, mock, cli)

			wait.Done()
		}()
	}
	wait.Wait()

	assert.NoError(t, pool.Close())
}

func TestKvClientPoolCreates5Connections(t *testing.T) {
	clientConfig := KvClientConfig{
		Address:        "endpoint1",
		TlsConfig:      nil,
		SelectedBucket: "test",
		Authenticator: &PasswordAuthenticator{
			Username: "username",
			Password: "password",
		},
	}
	var called uint32
	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &clientConfig, config)

			atomic.AddUint32(&called, 1)

			return makeMockKvClient(), nil
		},
	})
	require.NoError(t, err)

	// This should basically be instant.
	assert.Eventually(t, func() bool {
		return atomic.LoadUint32(&called) == uint32(5)
	}, 50*time.Millisecond, 1*time.Millisecond)

	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.NoError(t, pool.Close())
}

func TestKvClientPoolReconfigure(t *testing.T) {
	mock := &KvClientMock{
		ReconfigureFunc: func(opts *KvClientConfig, cb func(error)) error {
			cb(nil)
			return nil
		},
		CloseFunc: func() error { return nil },
	}
	clientConfig := KvClientConfig{
		Address:        "endpoint1",
		TlsConfig:      nil,
		SelectedBucket: "test",
		Authenticator: &PasswordAuthenticator{
			Username: "username",
			Password: "password",
		},
	}

	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 3,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &clientConfig, config)

			return mock, nil
		},
	})
	require.NoError(t, err)

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	err = pool.Reconfigure(&KvClientPoolConfig{
		NumConnections: 1,
		ClientConfig:   clientConfig,
	}, func(error) {})
	require.NoError(t, err)

	cli, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	assert.NoError(t, pool.Close())
}

func TestKvClientPoolNewAndGetRace(t *testing.T) {
	clientConfig := KvClientConfig{
		Address:        "endpoint1",
		TlsConfig:      nil,
		SelectedBucket: "test",
		Authenticator: &PasswordAuthenticator{
			Username: "username",
			Password: "password",
		},
	}
	expectedErr := errors.New("connect failure")
	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 1,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			return nil, expectedErr
		},
	})
	require.NoError(t, err)

	_, err = pool.GetClient(context.Background())
	require.ErrorIs(t, err, expectedErr)

	assert.NoError(t, pool.Close())
}

// This test effectively just checks that pool close does not leak clients during normal operation.
func TestKvClientPoolClose(t *testing.T) {
	testutils.SkipIfShortTest(t)

	auth := &PasswordAuthenticator{
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	}
	clientConfig := KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		TlsConfig:      nil,
		SelectedBucket: testutils.TestOpts.BucketName,
		Authenticator:  auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		Logger: logger,
	})
	require.NoError(t, err)
	defer pool.Close()

	// Check that we've connected at least 1 client
	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	err = pool.Close()
	require.NoError(t, err)

	// Check that getting a client fails after close.
	_, err = pool.GetClient(context.Background())
	require.Error(t, err)
}

func TestKvClientPoolCloseAfterReconfigure(t *testing.T) {
	testutils.SkipIfShortTest(t)

	auth := &PasswordAuthenticator{
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	}
	clientConfig := KvClientConfig{
		Address:       testutils.TestOpts.MemdAddrs[0],
		TlsConfig:     nil,
		Authenticator: auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := NewKvClientPool(&KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig:   clientConfig,
	}, &KvClientPoolOptions{
		Logger: logger,
	})
	require.NoError(t, err)

	// Check that we've connected at least 1 client.
	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	err = pool.Reconfigure(&KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig: KvClientConfig{
			Address:        testutils.TestOpts.MemdAddrs[0],
			TlsConfig:      nil,
			SelectedBucket: testutils.TestOpts.BucketName,
			Authenticator:  auth,
		},
	}, func(err error) {
		// We don't need to wait for all of the clients to be fully reconfigured.
	})
	require.NoError(t, err)

	err = pool.Close()
	require.NoError(t, err)
}

func makeMockKvClient() KvClient {
	return &KvClientMock{
		CloseFunc: func() error {
			return nil
		},
	}
}
