package core

import (
	"context"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"go.uber.org/zap"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentBasic(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	upsertRes, err := agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	require.NoError(t, err)
	assert.NotZero(t, upsertRes.Cas)

	getRes, err := agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, getRes.Cas)
	assert.NotEmpty(t, getRes.Value)
}

func TestAgentBadCollection(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	opts := AgentOptions{
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	_, err = agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "invalid-scope",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownScopeName)

	_, err = agent.Get(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "_default",
		CollectionName: "invalid-collection",
	})
	require.ErrorIs(t, err, memdx.ErrUnknownCollectionName)
}

func TestAgentBasicHTTP(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	logger, _ := zap.NewDevelopment()

	opts := AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	resp, err := agent.SendHTTPRequest(context.Background(), &HTTPRequest{
		Service: MgmtService,
		Path:    "/pools/default/nodeServices",
	})
	require.NoError(t, err)

	assert.Equal(t, 200, resp.Raw.StatusCode)

	err = resp.Raw.Body.Close()
	require.NoError(t, err)
}

func BenchmarkBasicGet(b *testing.B) {
	opts := AgentOptions{
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	if err != nil {
		b.Errorf("failed to create agent: %s", err)
	}

	_, err = agent.Upsert(context.Background(), &UpsertOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
		Value:          []byte(`{"foo": "bar"}`),
	})
	if err != nil {
		b.Errorf("failed to upsert test document: %s", err)
	}

	for n := 0; n < b.N; n++ {
		agent.Get(context.Background(), &GetOptions{
			Key:            []byte("test"),
			ScopeName:      "",
			CollectionName: "",
		})
	}
	b.ReportAllocs()
}
