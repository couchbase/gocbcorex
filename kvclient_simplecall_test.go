package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestKvClientSimpleCallCancellationReturnTrue(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			return &memdxPendingOpMock{
				cancelled:   true,
				cancelledCh: make(chan struct{}, 1),
			}, nil
		},
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:  logger,
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	setRes, err := cli.Set(ctx, &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, setRes)

	ctx2, cancel2 := context.WithDeadline(context.Background(), time.Now())
	defer cancel2()

	setRes, err = cli.Set(ctx2, &memdx.SetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
		Value:     []byte("test"),
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, setRes)
}

func TestKvClientSimpleCallCancellationReturnFalse(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	expectedPacket := &memdx.Packet{
		Value:  []byte("value"),
		Extras: make([]byte, 4),
		Cas:    123,
	}
	memdxCli := &MemdxDispatcherCloserMock{
		DispatchFunc: func(packet *memdx.Packet, dispatchCallback memdx.DispatchCallback) (memdx.PendingOp, error) {
			op := &memdxPendingOpMock{
				cancelled:   false,
				cancelledCh: make(chan struct{}, 1),
			}

			go func() {
				<-op.cancelledCh
				dispatchCallback(expectedPacket, nil)
			}()
			return op, nil
		},
	}

	cli, err := NewKvClient(context.Background(), &KvClientConfig{
		Logger:  logger,
		Address: "endpoint1",
		Authenticator: &PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		},
		SelectedBucket: "bucket",
		NewMemdxClient: func(opts *memdx.ClientOptions) MemdxDispatcherCloser {
			return memdxCli
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	getRes, err := cli.Get(ctx, &memdx.GetRequest{
		Key:       []byte(uuid.NewString()),
		VbucketID: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedPacket.Cas, getRes.Cas)
	assert.Equal(t, expectedPacket.Value, getRes.Value)

}
