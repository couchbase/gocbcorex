package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/leakcheck"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/testutilsint"
	"go.uber.org/zap"
)

func CreateDefaultBucketsTrackingManagerOptions() gocbcorex.BucketsTrackingAgentManagerOptions {
	logger, _ := zap.NewDevelopment()

	return gocbcorex.BucketsTrackingAgentManagerOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
	}
}

func TestOnDemandAgentManagerClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.OnDemandAgentManagerOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
	}

	mgr, err := gocbcorex.CreateOnDemandAgentManager(context.Background(), opts)
	require.NoError(t, err)

	agent, err := mgr.GetClusterAgent()
	require.NoError(t, err)

	_, err = agent.Query(context.Background(), &gocbcorex.QueryOptions{
		Statement: "SELECT 1=1",
	})
	require.NoError(t, err)

	agent, err = mgr.GetBucketAgent(context.Background(), testutilsint.TestOpts.BucketName)
	require.NoError(t, err)

	_, err = agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:   []byte(uuid.NewString()[:6]),
		Value: []byte(uuid.NewString()),
	})
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	_, err = mgr.GetClusterAgent()
	assert.Error(t, err)

	_, err = mgr.GetBucketAgent(context.Background(), testutilsint.TestOpts.BucketName)
	assert.Error(t, err)

	require.False(t, leakcheck.ReportLeakedGoroutines())
}

func TestBucketsTrackingAgentManagerClose(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultBucketsTrackingManagerOptions()

	mgr, err := gocbcorex.CreateBucketsTrackingAgentManager(context.Background(), opts)
	require.NoError(t, err)

	agent, err := mgr.GetClusterAgent(context.Background())
	require.NoError(t, err)

	_, err = agent.Query(context.Background(), &gocbcorex.QueryOptions{
		Statement: "SELECT 1=1",
	})
	require.NoError(t, err)

	agent, err = mgr.GetBucketAgent(context.Background(), testutilsint.TestOpts.BucketName)
	require.NoError(t, err)

	_, err = agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:   []byte(uuid.NewString()[:6]),
		Value: []byte(uuid.NewString()),
	})
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	_, err = mgr.GetClusterAgent(context.Background())
	assert.Error(t, err)

	_, err = mgr.GetBucketAgent(context.Background(), testutilsint.TestOpts.BucketName)
	assert.Error(t, err)

	require.False(t, leakcheck.ReportLeakedGoroutines())
}

func TestBucketsTrackingAgentManagerBucketNotExist(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	opts := CreateDefaultBucketsTrackingManagerOptions()

	mgr, err := gocbcorex.CreateBucketsTrackingAgentManager(context.Background(), opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()

	agent, err := mgr.GetBucketAgent(ctx, uuid.NewString()[:6])
	assert.ErrorIs(t, err, cbmgmtx.ErrBucketNotFound)

	if agent != nil {
		err = agent.Close()
		require.NoError(t, err)
	}

	err = mgr.Close()
	require.NoError(t, err)

	require.False(t, leakcheck.ReportLeakedGoroutines())
}
