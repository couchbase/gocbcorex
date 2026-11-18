package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/memdx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVbucketRouterDispatchToKey(t *testing.T) {
	dispatcher := NewVbucketRouter(nil)
	routingInfo := &VbucketRoutingInfo{
		VbMap: &VbucketMap{
			entries: [][]int{
				{
					0, 1,
				},
				{
					1, 0,
				},
				{
					0, 1,
				},
				{
					0, 1,
				},
				{
					1, 0,
				},
			},
			numReplicas: 1,
		},
		ServerList: []string{"endpoint1", "endpoint2"},
	}

	dispatcher.UpdateRoutingInfo(routingInfo)

	endpoint, vbID, err := dispatcher.DispatchByKey([]byte("key1"), 0)
	require.NoError(t, err)

	assert.Equal(t, "endpoint2", endpoint)
	assert.Equal(t, uint16(1), vbID)

	endpoint, vbID, err = dispatcher.DispatchByKey([]byte("key2"), 0)
	require.NoError(t, err)

	assert.Equal(t, "endpoint1", endpoint)
	assert.Equal(t, uint16(3), vbID)

	endpoint, vbID, err = dispatcher.DispatchByKey([]byte("key2"), 1)
	require.NoError(t, err)

	assert.Equal(t, "endpoint2", endpoint)
	assert.Equal(t, uint16(3), vbID)
}

func TestVbucketRouterDispatchToVbucketForward(t *testing.T) {
	dispatcher := NewVbucketRouter(nil)
	dispatcher.UpdateRoutingInfo(&VbucketRoutingInfo{
		VbMap: &VbucketMap{
			entries:     [][]int{{0, 1}, {0, 1}},
			numReplicas: 1,
		},
		ServerList: []string{"endpoint1", "endpoint2", "endpoint3"},
	})

	_, err := dispatcher.DispatchToVbucketForward(0, 0)
	assert.ErrorIs(t, err, ErrNoForwardVbucketMap)

	dispatcher.UpdateRoutingInfo(&VbucketRoutingInfo{
		VbMap: &VbucketMap{
			entries:     [][]int{{0, 1}, {0, 1}},
			numReplicas: 1,
		},
		VbMapForward: &VbucketMap{
			entries:     [][]int{{2, 1}, {-1, 1}},
			numReplicas: 1,
		},
		ServerList: []string{"endpoint1", "endpoint2", "endpoint3"},
	})

	endpoint, err := dispatcher.DispatchToVbucketForward(0, 0)
	require.NoError(t, err)
	assert.Equal(t, "endpoint3", endpoint)

	endpoint, err = dispatcher.DispatchToVbucketForward(0, 1)
	require.NoError(t, err)
	assert.Equal(t, "endpoint2", endpoint)

	_, err = dispatcher.DispatchToVbucketForward(1, 0)
	assert.ErrorIs(t, err, ErrNoServerAssigned)

	// the main map should be unaffected by the presence of a forward map
	endpoint, err = dispatcher.DispatchToVbucket(0, 0)
	require.NoError(t, err)
	assert.Equal(t, "endpoint1", endpoint)
}

// testNmvbConfigHandler applies not-my-vbucket configs to a router in the
// same way that the agent does.
type testNmvbConfigHandler struct {
	t      *testing.T
	router *vbucketRouter
}

func (h *testNmvbConfigHandler) HandleNotMyVbucketConfig(config *cbconfig.TerseConfigJson, sourceHostname string) {
	parsedConfig, err := ConfigParser{}.ParseTerseConfig(config, sourceHostname)
	require.NoError(h.t, err)

	compCfgs := GenerateComponentConfigsFromConfig(parsedConfig, "default", nil, "default", nil, nil)
	h.router.UpdateRoutingInfo(compCfgs.VbucketRoutingInfo)
}

func genTestRebalanceBucketConfig(rev int, vbMap, vbMapForward [][]int) *cbconfig.TerseConfigJson {
	cfg := GenTerseBucketConfig(rev, 1, []string{"host1", "host2", "host3"})
	cfg.VBucketServerMap.VBucketMap = vbMap
	cfg.VBucketServerMap.VBucketMapForward = vbMapForward
	return cfg
}

func genTestVbMap(numVbs int, entry ...int) [][]int {
	vbMap := make([][]int, numVbs)
	for i := range vbMap {
		vbMap[i] = append([]int(nil), entry...)
	}
	return vbMap
}

func makeTestNmvbError(t *testing.T, config *cbconfig.TerseConfigJson, remoteHostname string) error {
	configJson, err := json.Marshal(config)
	require.NoError(t, err)

	return &KvClientError{
		Cause: &memdx.ServerErrorWithConfig{
			Cause: memdx.ServerError{
				OpCode: memdx.OpCodeGet,
				Status: memdx.StatusNotMyVBucket,
				Cause:  memdx.ErrNotMyVbucket,
			},
			ConfigJson: configJson,
		},
		RemoteHostname: remoteHostname,
	}
}

func TestOrchestrateMemdRoutingFastForwardMap(t *testing.T) {
	const numVbs = 4
	testKey := []byte("key2")

	// vbuckets are all moving from host1 (active) to host3 (active), with
	// host2 as the replica throughout.
	vbMapBefore := genTestVbMap(numVbs, 0, 1)
	vbMapForward := genTestVbMap(numVbs, 2, 1)

	testVbID := VbucketMap{entries: vbMapBefore}.VbucketByKey(testKey)

	type routeFn func(
		vb VbucketRouter,
		ch NotMyVbucketConfigHandler,
		fn func(endpoint string, vbID uint16) (string, error),
	) (string, error)

	routeFns := map[string]routeFn{
		"ByKey": func(vb VbucketRouter, ch NotMyVbucketConfigHandler, fn func(string, uint16) (string, error)) (string, error) {
			return OrchestrateMemdRouting(context.Background(), vb, ch, testKey, 0, fn)
		},
		"ByVbucketId": func(vb VbucketRouter, ch NotMyVbucketConfigHandler, fn func(string, uint16) (string, error)) (string, error) {
			return OrchestrateMemdRoutingByVbucketId(context.Background(), vb, ch, testVbID, 0,
				func(endpoint string) (string, error) {
					return fn(endpoint, testVbID)
				})
		},
	}

	type testCase struct {
		Name string
		// the fast-forward map in the config returned with NOT_MY_VBUCKET
		NmvbForwardMap [][]int
		// whether the fast-forward owner also rejects the request
		ForwardOwnerRejects bool

		ExpectedAttempts []int
		ExpectOutdated   bool
	}

	testCases := []testCase{
		{
			Name:             "ForwardOwnerAccepts",
			NmvbForwardMap:   vbMapForward,
			ExpectedAttempts: []int{0, 2},
		},
		{
			Name:             "NoForwardMap",
			NmvbForwardMap:   nil,
			ExpectedAttempts: []int{0},
			ExpectOutdated:   true,
		},
		{
			Name:                "ForwardOwnerRejects",
			NmvbForwardMap:      vbMapForward,
			ForwardOwnerRejects: true,
			ExpectedAttempts:    []int{0, 2},
			ExpectOutdated:      true,
		},
		{
			Name:             "ForwardOwnerUnassigned",
			NmvbForwardMap:   genTestVbMap(numVbs, -1, 1),
			ExpectedAttempts: []int{0},
			ExpectOutdated:   true,
		},
		{
			Name:             "ForwardOwnerIsRejectingNode",
			NmvbForwardMap:   genTestVbMap(numVbs, 0, 1),
			ExpectedAttempts: []int{0},
			ExpectOutdated:   true,
		},
	}

	for routeName, route := range routeFns {
		for _, tc := range testCases {
			t.Run(routeName+"/"+tc.Name, func(t *testing.T) {
				router := NewVbucketRouter(nil)
				nmvbHandler := &testNmvbConfigHandler{t: t, router: router}

				// the client starts with a config from before the rebalance started
				nmvbHandler.HandleNotMyVbucketConfig(
					genTestRebalanceBucketConfig(1, vbMapBefore, nil), "host1")

				servers, err := router.GetServerList()
				require.NoError(t, err)
				require.Len(t, servers, 3)

				_, vbID, err := router.DispatchByKey(testKey, 0)
				require.NoError(t, err)
				require.Equal(t, testVbID, vbID)

				// the server has not published the new map yet, so the config it
				// sends with NOT_MY_VBUCKET still names host1 as the owner.
				nmvbConfig := genTestRebalanceBucketConfig(2, vbMapBefore, tc.NmvbForwardMap)

				var attempts []int
				res, err := route(router, nmvbHandler, func(endpoint string, vbID uint16) (string, error) {
					assert.Equal(t, testVbID, vbID)

					serverIdx := slices.Index(servers, endpoint)
					require.NotEqual(t, -1, serverIdx, "unexpected endpoint %s", endpoint)
					attempts = append(attempts, serverIdx)

					// bound the test in case of a routing loop
					require.LessOrEqual(t, len(attempts), 5, "too many routing attempts")

					switch serverIdx {
					case 0:
						return "", makeTestNmvbError(t, nmvbConfig, "host1")
					case 2:
						if tc.ForwardOwnerRejects {
							return "", makeTestNmvbError(t, nmvbConfig, "host3")
						}
						return "success", nil
					}

					return "", errors.New("request sent to replica node")
				})

				assert.Equal(t, tc.ExpectedAttempts, attempts)
				if tc.ExpectOutdated {
					assert.ErrorIs(t, err, ErrVbucketMapOutdated)
				} else {
					require.NoError(t, err)
					assert.Equal(t, "success", res)
				}
			})
		}
	}
}
