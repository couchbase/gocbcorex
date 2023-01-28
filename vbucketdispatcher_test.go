package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVbucketRouterDispatchToKey(t *testing.T) {
	dispatcher := newVbucketRouter()
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

	dispatcher.UpdateRoutingInfo(routingInfo)

	endpoint, vbID, err := dispatcher.DispatchByKey(context.Background(), []byte("key1"))
	require.NoError(t, err)

	assert.Equal(t, "endpoint2", endpoint)
	assert.Equal(t, uint16(1), vbID)

	endpoint, vbID, err = dispatcher.DispatchByKey(context.Background(), []byte("key2"))
	require.NoError(t, err)

	assert.Equal(t, "endpoint1", endpoint)
	assert.Equal(t, uint16(3), vbID)
}
