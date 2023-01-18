package core

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBasicSnAgent(t *testing.T) {
	t.SkipNow()

	opts := FakeAgentOptions{
		TLSConfig:  nil,
		BucketName: "default",
		Username:   "Administrator",
		Password:   "password",
		HTTPAddrs:  []string{"http://10.112.230.101:8091"},
		MemdAddrs:  []string{"10.112.230.101:11210"},
	}

	agent := CreateAgent(opts)

	respCh := make(chan *GetResult, 1)
	errCh := make(chan error, 1)
	err := agent.Get(&AsyncContext{}, GetOptions{
		Key:            []byte("test"),
		ScopeName:      "",
		CollectionName: "",
	}, func(result *GetResult, err error) {
		if err != nil {
			errCh <- err
			return
		}

		respCh <- result
	})
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case resp := <-respCh:
		assert.NotZero(t, resp.Cas)
		assert.NotEmpty(t, resp.Value)
	}
}
