package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVbucketRouterDispatchToKey(t *testing.T) {
	dispatcher := NewVbucketRouter(nil)
	routingInfo := &VbucketRoutingInfo{
		VbMap: &VbucketMap{
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
		ServerList: []string{"endpoint1", "endpoint2"},
	}

	dispatcher.UpdateRoutingInfo(routingInfo)

	endpoint, vbID, err := dispatcher.DispatchByKey([]byte("key1"))
	require.NoError(t, err)

	assert.Equal(t, "endpoint2", endpoint)
	assert.Equal(t, uint16(1), vbID)

	endpoint, vbID, err = dispatcher.DispatchByKey([]byte("key2"))
	require.NoError(t, err)

	assert.Equal(t, "endpoint1", endpoint)
	assert.Equal(t, uint16(3), vbID)
}