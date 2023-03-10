package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientPoolClientBasic(t *testing.T) {
	var wg sync.WaitGroup

	clientConfig := KvClientConfig{
		Address: "endpoint1",
	}
	clientConfig2 := KvClientConfig{
		Address: "endpoint2",
	}

	var mockClient *KvClientMock
	var mockClient2 *KvClientMock
	var mockClient3 *KvClientMock

	clientFactory := kvClientFactoryMock{
		NewKvClientFunc: func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			return &KvClientMock{
				ReconfigureFunc: func(config *KvClientConfig, cb func(error)) error {
					return errors.New("dont allow reconfigure")
				},
				ShutdownFunc: func(ctx context.Context) error {
					return nil
				},
				CloseFunc: func() error {
					return nil
				},
			}, nil
		},
	}

	var numNewKvClientCalls int32
	wg.Add(1)
	cp, err := newKvClientPoolClient(&clientConfig, &kvClientPoolClientOptions{
		NewKvClient: clientFactory.NewKvClient,
	}, func(err error, kc KvClient, okc KvClient) {
		require.NoError(t, err)
		require.Equal(t, &mockClient, kc)
		require.Equal(t, nil, okc)
		wg.Done()
	})
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, int32(1), atomic.LoadInt32(&numNewKvClientCalls))

	wg.Add(1)
	err = cp.Reconfigure(&clientConfig2, func(err error, kc KvClient, okc KvClient) {
		require.NoError(t, err)
		require.Equal(t, &mockClient2, kc)
		require.Equal(t, &mockClient, okc)
		wg.Done()
	})
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, int32(2), atomic.LoadInt32(&numNewKvClientCalls))
	require.Equal(t, 1, len(mockClient.ReconfigureCalls()))
	require.Equal(t, 0, len(mockClient2.ReconfigureCalls()))

	require.Eventually(t, func() bool {
		return len(mockClient.ShutdownCalls()) > 0
	}, 1*time.Second, 1*time.Millisecond)

	wg.Add(1)
	cp.Reset(func(err error, kc KvClient, okc KvClient) {
		require.NoError(t, err)
		require.Equal(t, &mockClient3, kc)
		require.Equal(t, nil, okc)
		wg.Done()
	})
	wg.Wait()

	require.Eventually(t, func() bool {
		return len(mockClient2.ShutdownCalls()) > 0
	}, 1*time.Second, 1*time.Millisecond)

	require.Equal(t, int32(3), atomic.LoadInt32(&numNewKvClientCalls))
	require.Equal(t, 1, len(mockClient.ReconfigureCalls()))
	require.Equal(t, 0, len(mockClient2.ReconfigureCalls()))
	require.Equal(t, 0, len(mockClient3.ReconfigureCalls()))

	cp.Shutdown(context.Background())

	// we have to sleep here to see the delayed shutdown call thats made inside
	// the manager when its cleaning up the clients before the synchronous shutdown
	// calls get made
	require.Greater(t, len(mockClient.ShutdownCalls()), 0)
	require.Greater(t, len(mockClient2.ShutdownCalls()), 0)
	require.Greater(t, len(mockClient3.ShutdownCalls()), 0)

	cp.Close()

	// everything should have been closed by Shutdown calls
	require.Equal(t, 0, len(mockClient.CloseCalls()))
	require.Equal(t, 0, len(mockClient2.CloseCalls()))
	require.Equal(t, 0, len(mockClient3.CloseCalls()))
}
