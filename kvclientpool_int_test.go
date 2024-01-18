package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// This test effectively just checks that pool close does not leak clients during normal operation.
func TestKvClientPoolClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}
	clientConfig := gocbcorex.KvClientConfig{
		Address:        testutilsint.TestOpts.MemdAddrs[0],
		TlsConfig:      nil,
		SelectedBucket: testutilsint.TestOpts.BucketName,
		Authenticator:  auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig:   clientConfig,
	}, &gocbcorex.KvClientPoolOptions{
		Logger: logger,
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
	require.Error(t, err)
}

func TestKvClientPoolCloseAfterReconfigure(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}
	clientConfig := gocbcorex.KvClientConfig{
		Address:       testutilsint.TestOpts.MemdAddrs[0],
		TlsConfig:     nil,
		Authenticator: auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig:   clientConfig,
	}, &gocbcorex.KvClientPoolOptions{
		Logger: logger,
	})
	require.NoError(t, err)

	// Check that we've connected at least 1 client.
	_, err = pool.GetClient(context.Background())
	require.NoError(t, err)

	err = pool.Reconfigure(&gocbcorex.KvClientPoolConfig{
		NumConnections: 5,
		ClientConfig: gocbcorex.KvClientConfig{
			Address:        testutilsint.TestOpts.MemdAddrs[0],
			TlsConfig:      nil,
			SelectedBucket: testutilsint.TestOpts.BucketName,
			Authenticator:  auth,
		},
	}, func(err error) {
		// We don't need to wait for all of the clients to be fully reconfigured.
	})
	require.NoError(t, err)

	err = pool.Close()
	require.NoError(t, err)
}

func TestKvClientPoolHandleClientClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	auth := &gocbcorex.PasswordAuthenticator{
		Username: testutilsint.TestOpts.Username,
		Password: testutilsint.TestOpts.Password,
	}
	clientConfig := &gocbcorex.KvClientConfig{
		Address:       testutilsint.TestOpts.MemdAddrs[0],
		TlsConfig:     nil,
		Authenticator: auth,
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	pool, err := gocbcorex.NewKvClientPool(&gocbcorex.KvClientPoolConfig{
		NumConnections: 1,
		ClientConfig:   *clientConfig,
	}, &gocbcorex.KvClientPoolOptions{
		Logger: logger,
	})
	require.NoError(t, err)

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	// Close the client out of band and check that the pool fetches a new client.
	err = cli.Close()
	require.NoError(t, err)

	// The read side close handling happens in a different goroutine to Close so we need
	// to Eventually this.
	require.Eventually(t, func() bool {
		cli2, err := pool.GetClient(context.Background())
		if err != nil {
			t.Logf("failed to get client %s", err)
			return false
		}

		return cli2 != cli
	}, 10*time.Second, 100*time.Millisecond)

	err = pool.Close()
	require.NoError(t, err)
}
