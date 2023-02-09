package core

import (
	"context"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func CreateDefaultAgent(t *testing.T) *Agent {
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

	return agent
}

func TestAgentBasic(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

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

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentBadCollection(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

	_, err := agent.Get(context.Background(), &GetOptions{
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

	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentBasicHTTP(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	agent := CreateDefaultAgent(t)

	resp, err := agent.SendHTTPRequest(context.Background(), &HTTPRequest{
		Service: MgmtService,
		Path:    "/pools/default/nodeServices",
	})
	require.NoError(t, err)

	assert.Equal(t, 200, resp.Raw.StatusCode)

	err = resp.Raw.Body.Close()
	require.NoError(t, err)

	err = agent.Close()
	require.NoError(t, err)
}

func BenchmarkBasicGet(b *testing.B) {
	opts := AgentOptions{
		TLSConfig:  nil,
		BucketName: testutils.TestOpts.BucketName,
		Username:   testutils.TestOpts.Username,
		Password:   testutils.TestOpts.Password,
		HTTPAddrs:  testutils.TestOpts.HTTPAddrs,
		MemdAddrs:  testutils.TestOpts.MemdAddrs,
	}

	agent, err := CreateAgent(context.Background(), opts)
	if err != nil {
		b.Errorf("failed to create agent: %s", err)
	}
	defer agent.Close()

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
