package gocbcorex

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapEndpointClientProvider routes endpoints to specific KvClients.
type mapEndpointClientProvider struct {
	clients map[string]KvClient
}

func (p *mapEndpointClientProvider) GetEndpointClient(_ context.Context, endpoint string) (KvClient, error) {
	c, ok := p.clients[endpoint]
	if !ok {
		return nil, InvalidEndpointError{Endpoint: endpoint}
	}
	return c, nil
}

func makeStatsMock(entries []memdx.StatsDataResponse, retErr error) *KvClientMock {
	return &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
		StatsFunc: func(_ context.Context, _ *memdx.StatsRequest, dataCb func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error) {
			for i := range entries {
				_ = dataCb(&entries[i])
			}
			return &memdx.StatsActionResponse{}, retErr
		},
	}
}

func newStatsCrudComponent(vbs VbucketRouter, ecp KvEndpointClientProvider) *CrudComponent {
	return &CrudComponent{
		retries:         NewRetryManagerFastFail(),
		vbs:             vbs,
		eclientProvider: ecp,
	}
}

func TestStatsByKeySingleNode(t *testing.T) {
	kv := makeStatsMock([]memdx.StatsDataResponse{
		{Key: "key1", Value: "val1"},
		{Key: "key2", Value: "val2"},
	}, nil)

	vbs := &VbucketRouterMock{
		GetServerListFunc: func() ([]string, error) {
			return []string{"ep1"}, nil
		},
	}
	cc := newStatsCrudComponent(vbs, benchEndpointClientProvider{client: kv})

	var got []StatsDataResult
	err := cc.StatsByKey(context.Background(), &StatsByKeyOptions{GroupName: "items"}, func(r StatsDataResult) {
		got = append(got, r)
	})

	require.NoError(t, err)
	assert.Equal(t, []StatsDataResult{{Key: "key1", Value: "val1"}, {Key: "key2", Value: "val2"}}, got)
	require.Len(t, kv.StatsCalls(), 1)
	assert.Equal(t, "items", kv.StatsCalls()[0].Req.GroupName)
}

func TestStatsByKeyMultipleNodes(t *testing.T) {
	kv1 := makeStatsMock([]memdx.StatsDataResponse{{Key: "ep1-a", Value: "1"}}, nil)
	kv2 := makeStatsMock([]memdx.StatsDataResponse{{Key: "ep2-a", Value: "2"}}, nil)

	vbs := &VbucketRouterMock{
		GetServerListFunc: func() ([]string, error) {
			return []string{"ep1", "ep2"}, nil
		},
	}
	ecp := &mapEndpointClientProvider{clients: map[string]KvClient{"ep1": kv1, "ep2": kv2}}
	cc := newStatsCrudComponent(vbs, ecp)

	var got []StatsDataResult
	err := cc.StatsByKey(context.Background(), &StatsByKeyOptions{GroupName: "collections"}, func(r StatsDataResult) {
		got = append(got, r)
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, []StatsDataResult{
		{Key: "ep1-a", Value: "1"},
		{Key: "ep2-a", Value: "2"},
	}, got)
	assert.Len(t, kv1.StatsCalls(), 1)
	assert.Len(t, kv2.StatsCalls(), 1)
}

func TestStatsByKeyGetServerListError(t *testing.T) {
	serverListErr := errors.New("routing unavailable")
	vbs := &VbucketRouterMock{
		GetServerListFunc: func() ([]string, error) {
			return nil, serverListErr
		},
	}
	cc := newStatsCrudComponent(vbs, benchEndpointClientProvider{})

	err := cc.StatsByKey(context.Background(), &StatsByKeyOptions{}, func(StatsDataResult) {})

	require.ErrorIs(t, err, serverListErr)
}

func TestStatsByKeyNodeError(t *testing.T) {
	nodeErr := errors.New("stats unavailable")
	kv := makeStatsMock(nil, nodeErr)

	vbs := &VbucketRouterMock{
		GetServerListFunc: func() ([]string, error) {
			return []string{"ep1"}, nil
		},
	}
	cc := newStatsCrudComponent(vbs, benchEndpointClientProvider{client: kv})

	err := cc.StatsByKey(context.Background(), &StatsByKeyOptions{}, func(StatsDataResult) {})

	require.Error(t, err)
	assert.ErrorContains(t, err, nodeErr.Error())
}

func TestStatsByKeyOnBehalfOfForwarded(t *testing.T) {
	var capturedReq *memdx.StatsRequest
	kv := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
		StatsFunc: func(_ context.Context, req *memdx.StatsRequest, _ func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error) {
			capturedReq = req
			return &memdx.StatsActionResponse{}, nil
		},
	}

	vbs := &VbucketRouterMock{
		GetServerListFunc: func() ([]string, error) {
			return []string{"ep1"}, nil
		},
	}
	cc := newStatsCrudComponent(vbs, benchEndpointClientProvider{client: kv})

	err := cc.StatsByKey(context.Background(), &StatsByKeyOptions{
		GroupName:  "items",
		OnBehalfOf: "user1",
	}, func(StatsDataResult) {})

	require.NoError(t, err)
	require.NotNil(t, capturedReq)
	assert.Equal(t, "items", capturedReq.GroupName)
	assert.Equal(t, "user1", capturedReq.OnBehalfOf)
}
