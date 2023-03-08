//go:build !race

// Tests in this file concern ensuring that we are returning items to the syncCrudResulterPool correctly.
// sync.Pool itself does some funny stuff involving randomly not returning items to the pool when the race
// detector is enabled.
package gocbcorex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKvClientSimpleCallSuccessBehaviour(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	expectedPacket := &memdx.Packet{
		Value:  []byte("value"),
		Extras: make([]byte, 4),
		Cas:    123,
	}
	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			time.AfterFunc(1, func() {
				dispatchCallback(expectedPacket, nil)
			})
			return memdxPendingOpMock{
				cancelledCh: make(chan error, 1),
			}, nil
		},
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

	for {
		// Drain the pool
		if syncCrudResulterPool.Get() == nil {
			break
		}
	}

	res, err := cli.Get(context.Background(), &memdx.GetRequest{
		Key: []byte("tes"),
	})
	require.NoError(t, err)

	assert.Equal(t, res.Value, expectedPacket.Value)
	assert.Equal(t, res.Cas, expectedPacket.Cas)

	var numResulters int
	for {
		// Get all the resulters from the pool, which should be just the one.
		resulter := syncCrudResulterPool.Get()
		if resulter == nil {
			break
		}
		_, ok := resulter.(*syncCrudResulter)
		assert.True(t, ok)
		numResulters++
	}

	assert.Equal(t, 1, numResulters)
}

func TestKvClientSimpleCallCallbackFailureBehaviour(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	expectedErr := errors.New("some rubbish")
	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			time.AfterFunc(1, func() {
				dispatchCallback(nil, expectedErr)
			})
			return memdxPendingOpMock{
				cancelledCh: make(chan error, 1),
			}, nil
		},
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

	for {
		// Drain the pool
		if syncCrudResulterPool.Get() == nil {
			break
		}
	}

	_, err = cli.Get(context.Background(), &memdx.GetRequest{
		Key: []byte("tes"),
	})
	require.Error(t, err)

	var numResulters int
	for {
		// Get all the resulters from the pool, which should be just the one.
		resulter := syncCrudResulterPool.Get()
		if resulter == nil {
			break
		}
		_, ok := resulter.(*syncCrudResulter)
		assert.True(t, ok)
		numResulters++
	}

	assert.Equal(t, 1, numResulters)
}

func TestKvClientSimpleCallCallFailureBehaviour(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	expectedErr := errors.New("some rubbish")
	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return nil, expectedErr
		},
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

	for {
		// Drain the pool
		if syncCrudResulterPool.Get() == nil {
			break
		}
	}

	_, err = cli.Get(context.Background(), &memdx.GetRequest{
		Key: []byte("tes"),
	})
	require.Error(t, err)

	var numResulters int
	for {
		// Get all the resulters from the pool, which should be just the one.
		resulter := syncCrudResulterPool.Get()
		if resulter == nil {
			break
		}
		_, ok := resulter.(*syncCrudResulter)
		assert.True(t, ok)
		numResulters++
	}

	assert.Equal(t, 1, numResulters)
}

func TestKvClientSimpleCallContextCancelBehaviour(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			op := memdxPendingOpMock{
				cancelledCh: make(chan error, 1),
			}
			go func() {
				err := <-op.cancelledCh
				dispatchCallback(nil, err)
			}()
			return op, nil
		},
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

	for {
		// Drain the pool
		if syncCrudResulterPool.Get() == nil {
			break
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cli.Get(ctx, &memdx.GetRequest{
		Key: []byte("tes"),
	})
	require.ErrorIs(t, err, context.Canceled)

	var numResulters int
	for {
		// Get all the resulters from the pool, which should be just the one.
		resulter := syncCrudResulterPool.Get()
		if resulter == nil {
			break
		}
		_, ok := resulter.(*syncCrudResulter)
		assert.True(t, ok)
		numResulters++
	}

	assert.Equal(t, 1, numResulters)
}
