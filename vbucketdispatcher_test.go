package core

import (
	"context"
	"testing"
	"time"

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

// TODO: This test needs to somehow become more deterministic.
func TestVbucketRouterWaitToDispatch(t *testing.T) {
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

	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = dispatcher.DispatchByKey(context.Background(), []byte("key1"))
		wait <- struct{}{}
	}()

	dispatcher.UpdateRoutingInfo(routingInfo)

	<-wait

	require.NoError(t, err)

	assert.Equal(t, "endpoint2", endpoint)
	assert.Equal(t, uint16(1), vbID)
}

func TestVbucketRouterCancelWaitingToDispatch(t *testing.T) {
	dispatcher := newVbucketRouter()

	ctx, cancel := context.WithCancel(context.Background())
	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = dispatcher.DispatchByKey(ctx, []byte("key1"))
		wait <- struct{}{}
	}()

	cancel()

	<-wait

	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, "", endpoint)
	assert.Equal(t, uint16(0), vbID)
}

func TestVbucketRouterDeadlinedWaitingToDispatch(t *testing.T) {
	dispatcher := newVbucketRouter()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Millisecond))
	defer cancel()
	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = dispatcher.DispatchByKey(ctx, []byte("key1"))
		wait <- struct{}{}
	}()

	<-wait

	assert.ErrorIs(t, err, context.DeadlineExceeded)

	assert.Equal(t, "", endpoint)
	assert.Equal(t, uint16(0), vbID)
}
