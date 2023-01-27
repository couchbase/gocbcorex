package core

import (
	"fmt"
	"testing"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigManagerAppliesFirstConfig(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)
}

func TestConfigManagerAppliesNewerRevConfig(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)

	cfg = GenTerseClusterConfig(2, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)
}

func TestConfigManagerAppliesNewerRevEpochConfig(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)

	cfg = GenTerseClusterConfig(1, 2, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)
}

func TestConfigManagerIgnoresOlderConfig(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(2, 2, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)

	oldCfg := cfg
	cfg = GenTerseClusterConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, oldCfg, routeCfg)
}

func TestConfigManagerIgnoresInvalidConfig(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(2, 2, []string{"endpoint1", "endpoint2"})

	cfg.UUID = "123"
	cfg.NodeLocator = ""

	manager.ApplyConfig("endpoint1", cfg)
	require.Nil(t, routeCfg)
}

func TestConfigManagerAppliesBucketConfigOverCluster(t *testing.T) {
	manager := newConfigManager()
	var routeCfg *routeConfig
	manager.RegisterCallback(func(rc *routeConfig) {
		routeCfg = rc
	})

	cfg := GenTerseClusterConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)

	cfg = GenTerseBucketConfig(1, 1, []string{"endpoint1", "endpoint2"})

	manager.ApplyConfig("endpoint1", cfg)
	assertRouteConfigForTerseConfig(t, cfg, routeCfg)
}

/*
TODO(brett19): Disabled these tests because they don't make sense, but should be moved.
func TestConfigManagerDispatchToKey(t *testing.T) {
	manager := newConfigManager(nil)

	cfg := GenTerseBucketConfig(1, 1, []string{"endpoint1", "endpoint2"})
	_, applied := manager.ApplyConfig("endpoint1", cfg)
	require.True(t, applied)

	endpoint, vbID, err := manager.DispatchByKey(context.Background(), []byte("key1"))
	require.NoError(t, err)

	assert.Equal(t, "couchbase://endpoint1:11210", endpoint)
	assert.Equal(t, uint16(0x5c), vbID)

	endpoint, vbID, err = manager.DispatchByKey(context.Background(), []byte("key2"))
	require.NoError(t, err)

	assert.Equal(t, "couchbase://endpoint1:11210", endpoint)
	assert.Equal(t, uint16(0x155), vbID)

	err = manager.Close()
	require.NoError(t, err)
}

func TestConfigManagerDispatchToKeyAfterReconfigure(t *testing.T) {
	manager := newConfigManager(nil)

	cfg := GenTerseBucketConfig(1, 1, []string{"endpoint1", "endpoint2"})
	_, applied := manager.ApplyConfig("endpoint1", cfg)
	require.True(t, applied)

	endpoint, vbID, err := manager.DispatchByKey(context.Background(), []byte("key1"))
	require.NoError(t, err)

	assert.Equal(t, "couchbase://endpoint1:11210", endpoint)
	assert.Equal(t, uint16(0x5c), vbID)

	manager.Reconfigure(&tls.Config{})

	endpoint, vbID, err = manager.DispatchByKey(context.Background(), []byte("key2"))
	require.NoError(t, err)

	assert.Equal(t, "couchbases://endpoint1:11207", endpoint)
	assert.Equal(t, uint16(0x155), vbID)

	err = manager.Close()
	require.NoError(t, err)
}

// TODO: This test needs to somehow become more deterministic.
func TestConfigManagertDispatcherWaitToDispatch(t *testing.T) {
	manager := newConfigManager(nil)

	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = manager.DispatchByKey(context.Background(), []byte("key1"))
		wait <- struct{}{}
	}()

	cfg := GenTerseBucketConfig(1, 1, []string{"endpoint1", "endpoint2"})
	_, applied := manager.ApplyConfig("endpoint1", cfg)
	require.True(t, applied)

	<-wait

	require.NoError(t, err)

	assert.Equal(t, "couchbase://endpoint1:11210", endpoint)
	assert.Equal(t, uint16(0x5c), vbID)

	err = manager.Close()
	require.NoError(t, err)
}

func TestConfigManagerCancelWaitingToDispatch(t *testing.T) {
	manager := newConfigManager(nil)

	ctx, cancel := context.WithCancel(context.Background())
	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = manager.DispatchByKey(ctx, []byte("key1"))
		wait <- struct{}{}
	}()

	cancel()

	<-wait

	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, "", endpoint)
	assert.Equal(t, uint16(0), vbID)

	err = manager.Close()
	require.NoError(t, err)
}

func TestVbucketDispatcherDeadlinedWaitingToDispatch(t *testing.T) {
	manager := newConfigManager(nil)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Millisecond))
	defer cancel()
	var err error
	var endpoint string
	var vbID uint16
	wait := make(chan struct{}, 1)
	go func() {
		endpoint, vbID, err = manager.DispatchByKey(ctx, []byte("key1"))
		wait <- struct{}{}
	}()

	<-wait

	assert.ErrorIs(t, err, context.DeadlineExceeded)

	assert.Equal(t, "", endpoint)
	assert.Equal(t, uint16(0), vbID)

	err = manager.Close()
	require.NoError(t, err)
}
*/

// TODO: Once we consolidate all of our config stuff across projects we should also provide utils for generating configs.
func GenTerseClusterConfig(rev, revEpoch int, baseHostnames []string) *cbconfig.TerseConfigJson {
	cfg := &cbconfig.TerseConfigJson{
		Rev:                    rev,
		RevEpoch:               revEpoch,
		ClusterCapabilitiesVer: []int{1, 0},
		ClusterCapabilities: map[string][]string{
			"n1ql": {
				"enhancedPreparedStatements",
			},
		},
	}

	for i, host := range baseHostnames {
		cfg.NodesExt = append(cfg.NodesExt, cbconfig.TerseExtNodeJson{
			Services: map[string]int{
				"fts":     8094,
				"ftsSSL":  18094,
				"kv":      11210,
				"kvSSL":   11207,
				"n1ql":    8093,
				"n1qlSSL": 18093,
				"mgmt":    8091,
				"mgmtSSL": 18091,
			},
			ThisNode: i == 0,
			Hostname: host,
		})
	}

	return cfg
}

func GenTerseBucketConfig(rev, revEpoch int, baseHostnames []string) *cbconfig.TerseConfigJson {
	var vbmap [][]int
	for i := 0; i < 1024; i++ {
		entry := []int{0, 1}
		vbmap = append(vbmap, entry)
	}
	cfg := &cbconfig.TerseConfigJson{
		Rev:                   rev,
		RevEpoch:              revEpoch,
		Name:                  "default",
		NodeLocator:           "vbucket",
		UUID:                  "4a4b3a83519d5612d48117fb1eb84afd",
		URI:                   "/pools/default/buckets/default?bucket_uuid=4a4b3a83519d5612d48117fb1eb84afd",
		StreamingURI:          "/pools/default/bucketsStreaming/default?bucket_uuid=4a4b3a83519d5612d48117fb1eb84afd",
		BucketCapabilitiesVer: "",
		BucketCapabilities: []string{
			"collections",
			"durableWrite",
			"tombstonedUserXAttrs",
			"dcp",
			"cbhello",
			"touch",
			"cccp",
			"xdcrCheckpointing",
			"nodesExt",
			"xattr",
		},
		CollectionsManifestUid: "0",
		VBucketServerMap: &cbconfig.VBucketServerMapJson{
			HashAlgorithm: "CRC",
			NumReplicas:   1,
			VBucketMap:    vbmap,
		},
		ClusterCapabilitiesVer: []int{1, 0},
		ClusterCapabilities: map[string][]string{
			"n1ql": {
				"enhancedPreparedStatements",
			},
		},
	}

	for i, host := range baseHostnames {
		cfg.VBucketServerMap.ServerList = append(cfg.VBucketServerMap.ServerList, host+":11210")
		cfg.NodesExt = append(cfg.NodesExt, cbconfig.TerseExtNodeJson{
			Services: map[string]int{
				"fts":     8094,
				"ftsSSL":  18094,
				"kv":      11210,
				"kvSSL":   11207,
				"n1ql":    8093,
				"n1qlSSL": 18093,
				"mgmt":    8091,
				"mgmtSSL": 18091,
			},
			ThisNode: i == 0,
			Hostname: host,
		})
		cfg.Nodes = append(cfg.Nodes, cbconfig.TerseNodeJson{
			Hostname: host + ":8091",
			Ports: map[string]int{
				"direct": 11210,
			},
		})
	}

	return cfg
}

func assertRouteConfigForTerseConfig(t *testing.T, terse *cbconfig.TerseConfigJson, routeCfg *routeConfig) {
	var bktType bucketType
	if terse.Name == "" {
		bktType = bktTypeNone
	} else {
		bktType = bktTypeCouchbase
	}

	var kvList routeEndpoints
	var ftsList routeEndpoints
	var n1qlList routeEndpoints
	var mgmtList routeEndpoints
	for _, n := range terse.NodesExt {
		if n.Services["kv"] > 0 {
			kvList.NonSSLEndpoints = append(kvList.NonSSLEndpoints,
				fmt.Sprintf("couchbase://%s:%d", n.Hostname, n.Services["kv"]))
		}
		if n.Services["kvSSL"] > 0 {
			kvList.SSLEndpoints = append(kvList.SSLEndpoints,
				fmt.Sprintf("couchbases://%s:%d", n.Hostname, n.Services["kvSSL"]))
		}
		if n.Services["fts"] > 0 {
			ftsList.NonSSLEndpoints = append(ftsList.NonSSLEndpoints,
				fmt.Sprintf("http://%s:%d", n.Hostname, n.Services["fts"]))
		}
		if n.Services["ftsSSL"] > 0 {
			ftsList.SSLEndpoints = append(ftsList.SSLEndpoints,
				fmt.Sprintf("https://%s:%d", n.Hostname, n.Services["ftsSSL"]))
		}
		if n.Services["n1ql"] > 0 {
			n1qlList.NonSSLEndpoints = append(n1qlList.NonSSLEndpoints,
				fmt.Sprintf("http://%s:%d", n.Hostname, n.Services["n1ql"]))
		}
		if n.Services["n1qlSSL"] > 0 {
			n1qlList.SSLEndpoints = append(n1qlList.SSLEndpoints,
				fmt.Sprintf("https://%s:%d", n.Hostname, n.Services["n1qlSSL"]))
		}
		if n.Services["mgmt"] > 0 {
			mgmtList.NonSSLEndpoints = append(mgmtList.NonSSLEndpoints,
				fmt.Sprintf("http://%s:%d", n.Hostname, n.Services["mgmt"]))
		}
		if n.Services["mgmtSSL"] > 0 {
			mgmtList.SSLEndpoints = append(mgmtList.SSLEndpoints,
				fmt.Sprintf("https://%s:%d", n.Hostname, n.Services["mgmtSSL"]))
		}
	}

	assert.Equal(t, terse.Rev, int(routeCfg.revID))
	assert.Equal(t, terse.RevEpoch, int(routeCfg.revEpoch))
	assert.Equal(t, terse.UUID, routeCfg.uuid)
	assert.Equal(t, terse.Name, routeCfg.name)
	assert.Equal(t, bktType, routeCfg.bktType)
	assert.Equal(t, kvList, routeCfg.kvServerList)
	assert.Equal(t, ftsList, routeCfg.ftsEpList)
	assert.Equal(t, n1qlList, routeCfg.n1qlEpList)
	assert.Equal(t, mgmtList, routeCfg.mgmtEpList)
	if terse.VBucketServerMap == nil {
		assert.Nil(t, routeCfg.vbMap)
	} else {
		assert.Equal(t, terse.VBucketServerMap.VBucketMap, routeCfg.vbMap.entries)
		assert.Equal(t, terse.VBucketServerMap.NumReplicas, routeCfg.vbMap.numReplicas)
	}
	assert.Equal(t, terse.ClusterCapabilitiesVer, routeCfg.clusterCapabilitiesVer)
	assert.Equal(t, terse.ClusterCapabilities, routeCfg.clusterCapabilities)
	assert.Equal(t, terse.BucketCapabilitiesVer, routeCfg.bucketCapabilitiesVer)
	assert.Equal(t, terse.BucketCapabilities, routeCfg.bucketCapabilities)
}
