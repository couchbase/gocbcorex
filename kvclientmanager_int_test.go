package gocbcorex_test

import (
	"context"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClientManagerClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	endpointName := "endpoint1"

	mgr, err := gocbcorex.NewKvClientManager(
		&gocbcorex.KvClientManagerConfig{
			NumPoolConnections: 3,
			Clients: map[string]*gocbcorex.KvClientConfig{
				endpointName: {
					Address:        testutilsint.TestOpts.MemdAddrs[0],
					TlsConfig:      nil,
					SelectedBucket: testutilsint.TestOpts.BucketName,
					Authenticator:  auth,
				},
			},
		},
		&gocbcorex.KvClientManagerOptions{
			Logger: logger,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := mgr.Close()
		require.NoError(t, err)
	})

	// Check that we've connected at least 1 client
	_, err = mgr.GetClient(context.Background(), endpointName)
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	// Check that getting a client fails after close.
	_, err = mgr.GetClient(context.Background(), endpointName)
	require.Error(t, err)
}

func TestClientManagerCloseAfterReconfigure(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	endpointName := "endpoint1"

	mgr, err := gocbcorex.NewKvClientManager(
		&gocbcorex.KvClientManagerConfig{
			NumPoolConnections: 3,
			Clients: map[string]*gocbcorex.KvClientConfig{
				endpointName: {
					Address:       testutilsint.TestOpts.MemdAddrs[0],
					TlsConfig:     nil,
					Authenticator: auth,
				},
			},
		},
		&gocbcorex.KvClientManagerOptions{
			Logger: logger,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := mgr.Close()
		require.NoError(t, err)
	})

	// Check that we've connected at least 1 client
	_, err = mgr.GetClient(context.Background(), endpointName)
	require.NoError(t, err)

	err = mgr.Reconfigure(&gocbcorex.KvClientManagerConfig{
		NumPoolConnections: 3,
		Clients: map[string]*gocbcorex.KvClientConfig{
			endpointName: {
				Address:        testutilsint.TestOpts.MemdAddrs[0],
				TlsConfig:      nil,
				SelectedBucket: testutilsint.TestOpts.BucketName,
				Authenticator:  auth,
			},
		},
	})
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	// Check that getting a client fails after close.
	_, err = mgr.GetClient(context.Background(), endpointName)
	require.Error(t, err)
}
