package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicSnAgent(t *testing.T) {
	// t.SkipNow()

	opts := FakeAgentOptions{
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  []string{"http://10.112.230.101:8091"},
		MemdAddrs:  []string{"10.112.230.101:11210"},
	}

	agent, err := CreateAgent(opts)
	require.NoError(t, err)

	res, err := agent.SyncGet(context.Background(), &GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	})
	require.NoError(t, err)
	assert.NotZero(t, res.Cas)
	assert.NotEmpty(t, res.Value)
}
