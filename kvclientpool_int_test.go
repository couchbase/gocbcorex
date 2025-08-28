package gocbcorex_test

import (
	"context"
	"net"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// This test effectively just checks that pool close does not leak clients during normal operation.
func TestKvClientPoolClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolOptions{
		Logger: logger,

		NumConnections: 5,
		KvClientPoolConfig: gocbcorex.KvClientPoolConfig{
			KvClientManagerConfig: gocbcorex.KvClientManagerConfig{

				Address:        testutilsint.TestOpts.MemdAddrs[0],
				TlsConfig:      nil,
				SelectedBucket: testutilsint.TestOpts.BucketName,
				Authenticator: &gocbcorex.PasswordAuthenticator{
					Username: testutilsint.TestOpts.Username,
					Password: testutilsint.TestOpts.Password,
				},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := pool.Close()
		require.NoError(t, err)
	})

	// Check that we've connected at least 1 client
	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	err = pool.Close()
	require.NoError(t, err)

	// Check that getting a client fails after close.
	_, err = pool.GetClient(context.Background())
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestKvClientPoolCloseAfterReconfigure(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolOptions{
		Logger: logger,

		NumConnections: 5,
		KvClientPoolConfig: gocbcorex.KvClientPoolConfig{
			KvClientManagerConfig: gocbcorex.KvClientManagerConfig{
				Address:        testutilsint.TestOpts.MemdAddrs[0],
				TlsConfig:      nil,
				SelectedBucket: testutilsint.TestOpts.BucketName,
				Authenticator:  auth,
			},
		},
	})
	require.NoError(t, err)

	// Check that we've connected at least 1 client.
	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	pool.Reconfigure(gocbcorex.KvClientPoolConfig{
		KvClientManagerConfig: gocbcorex.KvClientManagerConfig{
			Address:        testutilsint.TestOpts.MemdAddrs[0],
			TlsConfig:      nil,
			SelectedBucket: "invalid-bucket",
			Authenticator:  auth,
		},
	})

	err = pool.Close()
	require.NoError(t, err)
}

func TestKvClientPoolHandleClientClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolOptions{
		Logger: logger,

		NumConnections: 1,
		KvClientPoolConfig: gocbcorex.KvClientPoolConfig{
			KvClientManagerConfig: gocbcorex.KvClientManagerConfig{

				Address:        testutilsint.TestOpts.MemdAddrs[0],
				TlsConfig:      nil,
				SelectedBucket: testutilsint.TestOpts.BucketName,
				Authenticator: &gocbcorex.PasswordAuthenticator{
					Username: testutilsint.TestOpts.Username,
					Password: testutilsint.TestOpts.Password,
				},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := pool.Close()
		require.NoError(t, err)
	})

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	// Close the client out of band and check that the pool fetches a new client.
	err = cli.Close()
	require.NoError(t, err)

	// Explicit close calls against the client will propagate synchronously to the
	// pool, so we should immediately get a different client on the next GetClient call.
	// The read side close handling happens in a different goroutine to Close so we need
	// to Eventually this.
	cli2, err := pool.GetClient(context.Background())
	if err != nil {
		require.NoError(t, err)
	}

	require.NotEqual(t, cli, cli2)

}
