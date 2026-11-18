package gocbcorex

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func LoadTestTerseConfig(t *testing.T, path string) *ParsedConfig {
	cfgBytes := testutils.LoadTestData(t, path)

	var cfg cbconfig.TerseConfigJson
	err := json.Unmarshal(cfgBytes, &cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	parsedConfig, err := ConfigParser{}.ParseTerseConfig(&cfg, "SOURCE_HOSTNAME")
	if err != nil {
		t.Fatal(err.Error())
	}

	return parsedConfig
}

func TestConfigParserAltAddresses(t *testing.T) {
	cfg := LoadTestTerseConfig(t, "bucket_config_with_external_addresses.json")

	assert.Equal(t, int64(1073), cfg.RevID)
	assert.Equal(t, int64(0), cfg.RevEpoch)

	assert.Equal(t, "default", cfg.BucketName)
	assert.Equal(t, "ee7160b1f5392bcdbfc085c98b460999", cfg.BucketUUID)
	assert.Equal(t, bktTypeCouchbase, cfg.BucketType)
	assert.NotNil(t, cfg.VbucketMap)

	assert.ElementsMatch(t, cfg.Nodes, []ParsedConfigNode{
		{
			HasData: true,
			Addresses: ParsedConfigAddresses{
				Hostname: "172.17.0.2",
				NonSSLPorts: ParsedConfigServicePorts{
					Kv:        11210,
					Mgmt:      8091,
					Views:     8092,
					Query:     8093,
					Search:    8094,
					Analytics: 0,
				},
				SSLPorts: ParsedConfigServicePorts{
					Kv:        11207,
					Mgmt:      18091,
					Views:     18092,
					Query:     18093,
					Search:    18094,
					Analytics: 0,
				},
			},
			AltAddresses: map[string]ParsedConfigAddresses{
				"external": {
					Hostname: "192.168.132.234",
					NonSSLPorts: ParsedConfigServicePorts{
						Kv:        32775,
						Mgmt:      32790,
						Views:     32789,
						Query:     32788,
						Search:    32787,
						Analytics: 0,
					},
					SSLPorts: ParsedConfigServicePorts{
						Kv:        32776,
						Mgmt:      32773,
						Views:     32772,
						Query:     32771,
						Search:    32770,
						Analytics: 0,
					},
				},
			},
		},
		{
			HasData: true,
			Addresses: ParsedConfigAddresses{
				Hostname: "172.17.0.3",
				NonSSLPorts: ParsedConfigServicePorts{
					Kv:        11210,
					Mgmt:      8091,
					Views:     8092,
					Query:     8093,
					Search:    8094,
					Analytics: 0,
				},
				SSLPorts: ParsedConfigServicePorts{
					Kv:        11207,
					Mgmt:      18091,
					Views:     18092,
					Query:     18093,
					Search:    18094,
					Analytics: 0,
				},
			},
			AltAddresses: map[string]ParsedConfigAddresses{
				"external": {
					Hostname: "192.168.132.234",
					NonSSLPorts: ParsedConfigServicePorts{
						Kv:        32799,
						Mgmt:      32814,
						Views:     32813,
						Query:     32812,
						Search:    32811,
						Analytics: 0,
					},
					SSLPorts: ParsedConfigServicePorts{
						Kv:        32800,
						Mgmt:      32797,
						Views:     32796,
						Query:     32795,
						Search:    32794,
						Analytics: 0,
					},
				},
			},
		},
		{
			HasData: true,
			Addresses: ParsedConfigAddresses{
				Hostname: "172.17.0.4",
				NonSSLPorts: ParsedConfigServicePorts{
					Kv:        11210,
					Mgmt:      8091,
					Views:     8092,
					Query:     8093,
					Search:    8094,
					Analytics: 0,
				},
				SSLPorts: ParsedConfigServicePorts{
					Kv:        11207,
					Mgmt:      18091,
					Views:     18092,
					Query:     18093,
					Search:    18094,
					Analytics: 0,
				},
			},
			AltAddresses: map[string]ParsedConfigAddresses{
				"external": {
					Hostname: "192.168.132.234",
					NonSSLPorts: ParsedConfigServicePorts{
						Kv:        32823,
						Mgmt:      32838,
						Views:     32837,
						Query:     32836,
						Search:    32835,
						Analytics: 0,
					},
					SSLPorts: ParsedConfigServicePorts{
						Kv:        32824,
						Mgmt:      32821,
						Views:     32820,
						Query:     32819,
						Search:    32818,
						Analytics: 0,
					},
				},
			},
		},
	})
}

func TestConfigParserForwardVbucketMap(t *testing.T) {
	parseJson := func(t *testing.T, vbServerMapJson string) *ParsedConfig {
		cfgJson := `{
			"rev": 10,
			"name": "default",
			"nodeLocator": "vbucket",
			"uuid": "4a4b3a83519d5612d48117fb1eb84afd",
			"nodesExt": [
				{"hostname": "host1", "services": {"kv": 11210, "mgmt": 8091}},
				{"hostname": "host2", "services": {"kv": 11210, "mgmt": 8091}}
			],
			"nodes": [
				{"hostname": "host1:8091", "ports": {"direct": 11210}},
				{"hostname": "host2:8091", "ports": {"direct": 11210}}
			],
			"vBucketServerMap": ` + vbServerMapJson + `
		}`

		var cfg cbconfig.TerseConfigJson
		require.NoError(t, json.Unmarshal([]byte(cfgJson), &cfg))

		parsedConfig, err := ConfigParser{}.ParseTerseConfig(&cfg, "SOURCE_HOSTNAME")
		require.NoError(t, err)

		return parsedConfig
	}

	t.Run("Present", func(t *testing.T) {
		cfg := parseJson(t, `{
			"hashAlgorithm": "CRC",
			"numReplicas": 1,
			"serverList": ["host1:11210", "host2:11210"],
			"vBucketMap": [[0, 1], [0, 1], [1, 0], [1, 0]],
			"vBucketMapForward": [[1, 0], [0, 1], [1, -1], [0, 1]]
		}`)

		require.NotNil(t, cfg.VbucketMap)
		require.NotNil(t, cfg.VbucketMapForward)
		assert.Equal(t, 4, cfg.VbucketMapForward.NumVbuckets())
		assert.Equal(t, 1, cfg.VbucketMapForward.NumReplicas())

		idx, err := cfg.VbucketMapForward.NodeByVbucket(0, 0)
		require.NoError(t, err)
		assert.Equal(t, 1, idx)

		idx, err = cfg.VbucketMapForward.NodeByVbucket(2, 1)
		require.NoError(t, err)
		assert.Equal(t, -1, idx)

		// the fast-forward map should also make it through to the routing info
		compCfgs := GenerateComponentConfigsFromConfig(cfg, "default", nil, "default", nil, nil)
		assert.Same(t, cfg.VbucketMap, compCfgs.VbucketRoutingInfo.VbMap)
		assert.Same(t, cfg.VbucketMapForward, compCfgs.VbucketRoutingInfo.VbMapForward)
	})

	t.Run("Absent", func(t *testing.T) {
		cfg := parseJson(t, `{
			"hashAlgorithm": "CRC",
			"numReplicas": 1,
			"serverList": ["host1:11210", "host2:11210"],
			"vBucketMap": [[0, 1], [0, 1], [1, 0], [1, 0]]
		}`)

		require.NotNil(t, cfg.VbucketMap)
		assert.Nil(t, cfg.VbucketMapForward)
	})

	t.Run("LengthMismatch", func(t *testing.T) {
		cfg := parseJson(t, `{
			"hashAlgorithm": "CRC",
			"numReplicas": 1,
			"serverList": ["host1:11210", "host2:11210"],
			"vBucketMap": [[0, 1], [0, 1], [1, 0], [1, 0]],
			"vBucketMapForward": [[1, 0], [0, 1]]
		}`)

		require.NotNil(t, cfg.VbucketMap)
		assert.Nil(t, cfg.VbucketMapForward)
	})
}
