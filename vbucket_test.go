package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVbucketDispatcherDispatchToKey(t *testing.T) {
	ctx := &AsyncContext{}
	dispatcher := newVbucketDispatcher()
	routingInfo := &vbucketRoutingInfo{
		vbmap: &vbucketMap{
			entries: [][]int{
				{
					0,
				},
				{
					1,
				},
				{
					0,
				},
				{
					0,
				},
				{
					1,
				},
			},
			numReplicas: 0,
		},
		serverList: []string{"endpoint1", "endpoint2"},
	}

	dispatcher.StoreVbucketRoutingInfo(routingInfo)

	endpointCh := make(chan string, 1)
	errCh := make(chan error, 1)
	err := dispatcher.DispatchByKey(ctx, []byte("key1"), func(endpoint string, vbID uint16, err error) {
		if err != nil {
			errCh <- err
			return
		}

		endpointCh <- endpoint
	})
	require.Nil(t, err)

	select {
	case err = <-errCh:
		require.Nil(t, err)
	case endpoint := <-endpointCh:
		assert.Equal(t, "endpoint2", endpoint)
	}

	err = dispatcher.DispatchByKey(ctx, []byte("key2"), func(endpoint string, vbID uint16, err error) {
		if err != nil {
			errCh <- err
			return
		}

		endpointCh <- endpoint
	})
	require.Nil(t, err)
	select {
	case err = <-errCh:
		require.Nil(t, err)
	case endpoint := <-endpointCh:
		assert.Equal(t, "endpoint1", endpoint)
	}

	dispatcher.Close()
}
