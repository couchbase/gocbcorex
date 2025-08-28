package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvClientPoolGetClient(t *testing.T) {
	mock := makeMockKvClient()
	pool, err := NewKvClientPool(&KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientOptions) (KvClient, error) {
			return mock, nil
		},

		NumConnections: 1,
		KvClientPoolConfig: KvClientPoolConfig{
			KvClientManagerConfig: KvClientManagerConfig{
				Address:        "endpoint1",
				TlsConfig:      nil,
				SelectedBucket: "test",
				Authenticator: &PasswordAuthenticator{
					Username: "username",
					Password: "password",
				},
			},
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

	pool, err := NewKvClientPool(&KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientOptions) (KvClient, error) {
			return mock, nil
		},

		NumConnections: 1,
		KvClientPoolConfig: KvClientPoolConfig{
			KvClientManagerConfig: KvClientManagerConfig{
				Address:        "endpoint1",
				TlsConfig:      nil,
				SelectedBucket: "test",
				Authenticator: &PasswordAuthenticator{
					Username: "username",
					Password: "password",
				},
			},
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
	var called uint32
	pool, err := NewKvClientPool(&KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientOptions) (KvClient, error) {
			atomic.AddUint32(&called, 1)
			return makeMockKvClient(), nil
		},

		NumConnections: 5,
		KvClientPoolConfig: KvClientPoolConfig{
			KvClientManagerConfig: KvClientManagerConfig{
				Address:        "endpoint1",
				TlsConfig:      nil,
				SelectedBucket: "test",
				Authenticator: &PasswordAuthenticator{
					Username: "username",
					Password: "password",
				},
			},
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

func TestKvClientPoolNewAndGetRace(t *testing.T) {
	expectedErr := errors.New("connect failure")

	pool, err := NewKvClientPool(&KvClientPoolOptions{
		NewKvClient: func(ctx context.Context, config *KvClientOptions) (KvClient, error) {
			return nil, expectedErr
		},

		NumConnections: 1,
		KvClientPoolConfig: KvClientPoolConfig{
			KvClientManagerConfig: KvClientManagerConfig{
				Address:        "endpoint1",
				TlsConfig:      nil,
				SelectedBucket: "test",
				Authenticator: &PasswordAuthenticator{
					Username: "username",
					Password: "password",
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = pool.GetClient(context.Background())
	require.ErrorIs(t, err, expectedErr)

	assert.NoError(t, pool.Close())
}

func makeMockKvClient() KvClient {
	return &KvClientMock{
		CloseFunc: func() error { return nil },
	}
}
