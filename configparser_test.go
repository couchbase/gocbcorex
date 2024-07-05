package gocbcorex

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
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

	assert.Equal(t, cfg.RevID, int64(1073))
	assert.Equal(t, cfg.RevEpoch, int64(0))

	assert.Equal(t, cfg.BucketName, "default")
	assert.Equal(t, cfg.BucketUUID, "ee7160b1f5392bcdbfc085c98b460999")
	assert.Equal(t, cfg.BucketType, bktTypeCouchbase)
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
