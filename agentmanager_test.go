package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/testutils"
	"go.uber.org/zap"
)

func CreateDefaultBucketsTrackingManagerOptions() BucketsTrackingAgentManagerOptions {
	logger, _ := zap.NewDevelopment()

	return BucketsTrackingAgentManagerOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
		CompressionConfig: CompressionConfig{
			EnableCompression: true,
		},
	}
}

func TestOnDemandAgentManagerClose(t *testing.T) {
	testutils.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := OnDemandAgentManagerOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
		CompressionConfig: CompressionConfig{
			EnableCompression: true,
		},
	}

	mgr, err := CreateOnDemandAgentManager(context.Background(), opts)
	require.NoError(t, err)

	agent, err := mgr.GetClusterAgent()
	require.NoError(t, err)

	_, err = agent.Query(context.Background(), &QueryOptions{
		Statement: "SELECT 1=1",
	})
	require.NoError(t, err)

	agent, err = mgr.GetBucketAgent(context.Background(), testutils.TestOpts.BucketName)
	require.NoError(t, err)

	_, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:   []byte(uuid.NewString()[:6]),
		Value: []byte(uuid.NewString()),
	})
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	_, err = mgr.GetClusterAgent()
	assert.Error(t, err)

	_, err = mgr.GetBucketAgent(context.Background(), testutils.TestOpts.BucketName)
	assert.Error(t, err)
}

func TestBucketsTrackingAgentManagerClose(t *testing.T) {
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultBucketsTrackingManagerOptions()

	mgr, err := CreateBucketsTrackingAgentManager(context.Background(), opts)
	require.NoError(t, err)

	agent, err := mgr.GetClusterAgent(context.Background())
	require.NoError(t, err)

	_, err = agent.Query(context.Background(), &QueryOptions{
		Statement: "SELECT 1=1",
	})
	require.NoError(t, err)

	agent, err = mgr.GetBucketAgent(context.Background(), testutils.TestOpts.BucketName)
	require.NoError(t, err)

	_, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:   []byte(uuid.NewString()[:6]),
		Value: []byte(uuid.NewString()),
	})
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	_, err = mgr.GetClusterAgent(context.Background())
	assert.Error(t, err)

	_, err = mgr.GetBucketAgent(context.Background(), testutils.TestOpts.BucketName)
	assert.Error(t, err)
}

func TestBucketsTrackingAgentManagerBucketNotExist(t *testing.T) {
	testutils.SkipIfShortTest(t)

	opts := CreateDefaultBucketsTrackingManagerOptions()

	mgr, err := CreateBucketsTrackingAgentManager(context.Background(), opts)
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
}
