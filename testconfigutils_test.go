package gocbcorex

import (
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

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
			Services: &cbconfig.TerseExtNodePortsJson{
				Fts:     8094,
				FtsSsl:  18094,
				Kv:      11210,
				KvSsl:   11207,
				N1ql:    8093,
				N1qlSsl: 18093,
				Mgmt:    8091,
				MgmtSsl: 18091,
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
			Services: &cbconfig.TerseExtNodePortsJson{
				Fts:     8094,
				FtsSsl:  18094,
				Kv:      11210,
				KvSsl:   11207,
				N1ql:    8093,
				N1qlSsl: 18093,
				Mgmt:    8091,
				MgmtSsl: 18091,
			},
			ThisNode: i == 0,
			Hostname: host,
		})
		cfg.Nodes = append(cfg.Nodes, cbconfig.TerseNodeJson{
			Hostname: host + ":8091",
			Ports: &cbconfig.TerseNodePortsJson{
				Direct: 11210,
			},
		})
	}

	return cfg
}
