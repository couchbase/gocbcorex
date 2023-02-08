package core

import (
	"context"
	"crypto/tls"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type memdxPendingOpMock struct {
}

func (mpo memdxPendingOpMock) Cancel() bool {
	return true
}

func TestKvClientReconfigureBucket(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:   logger,
		Address:  testutils.TestOpts.MemdAddrs[0],
		Username: testutils.TestOpts.Username,
		Password: testutils.TestOpts.Password,
	})
	require.NoError(t, err)

	// Select a bucket on a gcccp level request
	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:        testutils.TestOpts.MemdAddrs[0],
		Username:       testutils.TestOpts.Username,
		Password:       testutils.TestOpts.Password,
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
}

func TestKvClientReconfigureBucketOverExistingBucket(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
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
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:   "endpoint1",
		Username:  "user",
		Password:  "pass",
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
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:   "endpoint1",
		Username:  "user2",
		Password:  "pass",
		TlsConfig: &tls.Config{},
	})
	require.Error(t, err)
}

func TestKvClientReconfigurePassword(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:   "endpoint1",
		Username:  "user",
		Password:  "pass2",
		TlsConfig: &tls.Config{},
	})
	require.Error(t, err)
}

func TestKvClientReconfigureAddress(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: nil,
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	err = cli.Reconfigure(context.Background(), &KvClientConfig{
		Address:   "endpoint2",
		Username:  "user",
		Password:  "pass",
		TlsConfig: &tls.Config{},
	})
	require.Error(t, err)
}

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
			return memdxPendingOpMock{}, nil
		},
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:         logger,
		Address:        "endpoint1",
		Username:       "user",
		Password:       "pass",
		SelectedBucket: "bucket",
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
