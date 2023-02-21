package gocbcorex

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvClientPoolGetClient(t *testing.T) {
	mock := &KvClientMock{}
	opts := KvClientConfig{
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
		ClientOpts:     opts,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, options *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &opts, options)

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
}

func TestKvClientPoolGetClientConcurrent(t *testing.T) {
	mock := &KvClientMock{}
	opts := KvClientConfig{
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
		ClientOpts:     opts,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, options *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &opts, options)

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
}

func TestKvClientPoolCreates5Connections(t *testing.T) {
	opts := KvClientConfig{
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
		ClientOpts:     opts,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, options *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &opts, options)

			atomic.AddUint32(&called, 1)

			return &KvClientMock{}, nil
		},
	})
	require.NoError(t, err)

	// This should basically be instant.
	assert.Eventually(t, func() bool {
		return atomic.LoadUint32(&called) == uint32(5)
	}, 50*time.Millisecond, 1*time.Millisecond)

	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)
}

func TestKvClientPoolReconfigure(t *testing.T) {
	mock := &KvClientMock{
		ReconfigureFunc: func(ctx context.Context, opts *KvClientConfig) error {
			return nil
		},
	}
	opts := KvClientConfig{
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
		ClientOpts:     opts,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, options *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &opts, options)

			return mock, nil
		},
	})
	require.NoError(t, err)

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	err = pool.Reconfigure(&KvClientPoolConfig{
		NumConnections: 1,
		ClientOpts:     opts,
	})
	require.NoError(t, err)

	cli, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)
}

func TestKvClientPoolReconfigureNilOptions(t *testing.T) {
	mock := &KvClientMock{}
	opts := KvClientConfig{
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
		ClientOpts:     opts,
	}, &KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, options *KvClientConfig) (KvClient, error) {
			assert.Equal(t, &opts, options)

			return mock, nil
		},
	})
	require.NoError(t, err)

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)

	err = pool.Reconfigure(nil)
	require.Error(t, err)

	cli, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mock, cli)
}
