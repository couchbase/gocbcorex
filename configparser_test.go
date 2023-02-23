package gocbcorex

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/stretchr/testify/assert"
)

func LoadTestTerseConfig(t *testing.T, path string) *ParsedConfig {
	cfgBytes := LoadTestData(t, path)

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
	cfg := LoadTestTerseConfig(t, "testdata/bucket_config_with_external_addresses.json")

	assert.Equal(t, cfg.RevID, int64(1073))
	assert.Equal(t, cfg.RevEpoch, int64(0))

	assert.Equal(t, cfg.BucketName, "default")
	assert.Equal(t, cfg.BucketUUID, "ee7160b1f5392bcdbfc085c98b460999")
	assert.Equal(t, cfg.BucketType, bktTypeCouchbase)
	assert.NotNil(t, cfg.VbucketMap)

	assert.NotNil(t, cfg.Addresses)
	assert.ElementsMatch(t, cfg.Addresses.NonSSL.Kv,
		[]string{"172.17.0.2:11210", "172.17.0.3:11210", "172.17.0.4:11210"})
	assert.ElementsMatch(t, cfg.Addresses.SSL.Kv,
		[]string{"172.17.0.2:11207", "172.17.0.3:11207", "172.17.0.4:11207"})
	assert.ElementsMatch(t, cfg.Addresses.NonSSL.KvData,
		[]string{"172.17.0.2:11210", "172.17.0.3:11210", "172.17.0.4:11210"})
	assert.ElementsMatch(t, cfg.Addresses.SSL.KvData,
		[]string{"172.17.0.2:11207", "172.17.0.3:11207", "172.17.0.4:11207"})

	assert.Len(t, cfg.AlternateAddresses, 1)
	assert.NotNil(t, cfg.AlternateAddresses["external"])
	assert.ElementsMatch(t, cfg.AlternateAddresses["external"].NonSSL.Kv,
		[]string{"192.168.132.234:32775", "192.168.132.234:32799", "192.168.132.234:32823"})
	assert.ElementsMatch(t, cfg.AlternateAddresses["external"].SSL.KvData,
		[]string{"192.168.132.234:32776", "192.168.132.234:32800", "192.168.132.234:32824"})
	assert.ElementsMatch(t, cfg.AlternateAddresses["external"].NonSSL.KvData,
		[]string{"192.168.132.234:32775", "192.168.132.234:32799", "192.168.132.234:32823"})
	assert.ElementsMatch(t, cfg.AlternateAddresses["external"].SSL.KvData,
		[]string{"192.168.132.234:32776", "192.168.132.234:32800", "192.168.132.234:32824"})

	log.Printf("Addresses: %+v", cfg.Addresses)
	log.Printf("AltAddresses: %+v", cfg.AlternateAddresses["external"])
}
