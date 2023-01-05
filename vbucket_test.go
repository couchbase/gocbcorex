package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVbucketDispatcherDispatchToKey(t *testing.T) {
	ctx := &AsyncContext{}
	dispatcher := &vbucketDispatcher{}
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

	dispatcher.storeRoutingInfo(nil, routingInfo)

	endpoint, err := dispatcher.DispatchByKey(ctx, []byte("key1"))
	require.Nil(t, err)

	assert.Equal(t, "endpoint2", endpoint)

	endpoint, err = dispatcher.DispatchByKey(ctx, []byte("key2"))
	require.Nil(t, err)

	assert.Equal(t, "endpoint1", endpoint)
}
