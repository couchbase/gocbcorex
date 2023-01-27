package core

import (
	"context"
	"testing"

	"github.com/couchbase/stellar-nebula/core/testutils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicSnAgent(t *testing.T) {
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
