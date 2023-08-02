package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAgentDelete(t *testing.T) {
	testutils.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		BucketName: testutils.TestOpts.BucketName,
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent.Close()

	docKey := uuid.NewString()

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	deleteRes, err := agent.Delete(context.Background(), &DeleteOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, deleteRes.Cas)

	_, err = agent.Get(context.Background(), &GetOptions{
		Key:            []byte(docKey),
		ScopeName:      "",
		CollectionName: "",
	})
	require.ErrorIs(t, err, memdx.ErrDocNotFound)
}

func TestAgentDoesNotRetryMemdxInvalidArgs(t *testing.T) {
	testutils.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
		Logger:    logger,
		TLSConfig: nil,
		Authenticator: &PasswordAuthenticator{
			Username: testutils.TestOpts.Username,
			Password: testutils.TestOpts.Password,
		},
		BucketName: testutils.TestOpts.BucketName,
		SeedConfig: SeedConfig{
			HTTPAddrs: testutils.TestOpts.HTTPAddrs,
			MemdAddrs: testutils.TestOpts.MemdAddrs,
		},
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)
	defer agent.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = agent.RangeScanCreate(ctx, &RangeScanCreateOptions{
		VbucketID: 1,
	})
	require.ErrorIs(t, err, memdx.ErrInvalidArgument)
}
