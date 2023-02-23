package gocbcorex

import (
	"fmt"
	"strings"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

func parseConfigHostname(hostname string, sourceHostname string) string {
	if hostname == "" {
		// if no hostname is provided, we want to be using the source one
		return sourceHostname
	}

	if strings.Contains(hostname, ":") {
		// this appears to be an IPv6 address, wrap it for everyone else
		return "[" + hostname + "]"
	}
	return hostname
}

func parseConfigHostsInto(config *ParsedConfigAddresses, hostname string, ports *cbconfig.TerseExtNodePortsJson, kvHasData bool) {
	if ports.Kv > 0 {
		endpoint := fmt.Sprintf("%s:%d", hostname, ports.Kv)
		config.NonSSL.Kv = append(config.NonSSL.Kv, endpoint)
		if kvHasData {
			config.NonSSL.KvData = append(config.NonSSL.KvData, endpoint)
		}
	}
	if ports.KvSsl > 0 {
		endpoint := fmt.Sprintf("%s:%d", hostname, ports.KvSsl)
		config.SSL.Kv = append(config.SSL.Kv, endpoint)
		if kvHasData {
			config.SSL.KvData = append(config.SSL.KvData, endpoint)
		}
	}

	if ports.Mgmt > 0 {
		config.NonSSL.Mgmt = append(config.NonSSL.Mgmt, fmt.Sprintf("%s:%d", hostname, ports.Mgmt))
	}
	if ports.MgmtSsl > 0 {
		config.SSL.Mgmt = append(config.SSL.Mgmt, fmt.Sprintf("%s:%d", hostname, ports.MgmtSsl))
	}

	if ports.Capi > 0 {
		config.NonSSL.Views = append(config.NonSSL.Views, fmt.Sprintf("%s:%d", hostname, ports.Capi))
	}
	if ports.CapiSsl > 0 {
		config.SSL.Views = append(config.SSL.Views, fmt.Sprintf("%s:%d", hostname, ports.CapiSsl))
	}

	if ports.N1ql > 0 {
		config.NonSSL.Query = append(config.NonSSL.Query, fmt.Sprintf("%s:%d", hostname, ports.N1ql))
	}
	if ports.N1qlSsl > 0 {
		config.SSL.Query = append(config.SSL.Query, fmt.Sprintf("%s:%d", hostname, ports.N1qlSsl))
	}

	if ports.Fts > 0 {
		config.NonSSL.Search = append(config.NonSSL.Search, fmt.Sprintf("%s:%d", hostname, ports.Fts))
	}
	if ports.FtsSsl > 0 {
		config.SSL.Search = append(config.SSL.Search, fmt.Sprintf("%s:%d", hostname, ports.FtsSsl))
	}
}

type ConfigParser struct{}

func (p ConfigParser) ParseTerseConfig(config *cbconfig.TerseConfigJson, sourceHostname string) (*ParsedConfig, error) {
	var out ParsedConfig
	out.RevID = int64(config.Rev)
	out.RevEpoch = int64(config.RevEpoch)

	out.Addresses = &ParsedConfigAddresses{}
	lenNodes := len(config.Nodes)
	for i, node := range config.NodesExt {
		kvHasData := i < lenNodes
		nodeHostname := parseConfigHostname(node.Hostname, sourceHostname)
		parseConfigHostsInto(out.Addresses, nodeHostname, node.Services, kvHasData)

		for networkType, altAddrs := range node.AltAddresses {
			if out.AlternateAddresses == nil {
				out.AlternateAddresses = make(map[string]*ParsedConfigAddresses)
			}
			if out.AlternateAddresses[networkType] == nil {
				out.AlternateAddresses[networkType] = &ParsedConfigAddresses{}
			}

			altAddrsOut := out.AlternateAddresses[networkType]
			altHostname := parseConfigHostname(altAddrs.Hostname, nodeHostname)
			parseConfigHostsInto(altAddrsOut, altHostname, altAddrs.Ports, kvHasData)
		}
	}

	if config.Name != "" {
		out.BucketUUID = config.UUID
		out.BucketName = config.Name
		switch config.NodeLocator {
		case "ketama":
			out.BucketType = bktTypeMemcached
		case "vbucket":
			out.BucketType = bktTypeCouchbase
			out.VbucketMap = NewVbucketMap(
				config.VBucketServerMap.VBucketMap,
				config.VBucketServerMap.NumReplicas)
		default:
			out.BucketType = bktTypeInvalid
		}
	}

	return &out, nil
}
