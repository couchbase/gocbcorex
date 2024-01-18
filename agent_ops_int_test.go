package gocbcorex_test

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAgentDelete(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	docKey := uuid.NewString()

	upsertRes, err := agent.Upsert(context.Background(), &gocbcorex.UpsertOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	deleteRes, err := agent.Delete(context.Background(), &gocbcorex.DeleteOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, deleteRes.Cas)

	_, err = agent.Get(context.Background(), &gocbcorex.GetOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.ErrorIs(t, err, memdx.ErrDocNotFound)
}

func TestAgentDoesNotRetryMemdxInvalidArgs(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := gocbcorex.AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		BucketName: testutilsint.TestOpts.BucketName,
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = agent.RangeScanCreate(ctx, &gocbcorex.RangeScanCreateOptions{
		VbucketID: 1,
	})
	require.ErrorIs(t, err, memdx.ErrInvalidArgument)
}
