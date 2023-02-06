package core

import (
	"context"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAgentDelete(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: testutils.TestOpts.BucketName,
		Username:   testutils.TestOpts.Username,
		Password:   testutils.TestOpts.Password,
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

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
