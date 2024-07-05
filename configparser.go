package gocbcorex

import (
	"strings"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"golang.org/x/exp/slices"
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

func parseConfigHostsInto(hostname string, ports *cbconfig.TerseExtNodePortsJson) ParsedConfigAddresses {
	var config ParsedConfigAddresses

	config.Hostname = hostname

	config.NonSSLPorts.Kv = int(ports.Kv)
	config.NonSSLPorts.Mgmt = int(ports.Mgmt)
	config.NonSSLPorts.Views = int(ports.Capi)
	config.NonSSLPorts.Query = int(ports.N1ql)
	config.NonSSLPorts.Search = int(ports.Fts)
	config.NonSSLPorts.Analytics = int(ports.Cbas)

	config.SSLPorts.Kv = int(ports.KvSsl)
	config.SSLPorts.Mgmt = int(ports.MgmtSsl)
	config.SSLPorts.Views = int(ports.CapiSsl)
	config.SSLPorts.Query = int(ports.N1qlSsl)
	config.SSLPorts.Search = int(ports.FtsSsl)
	config.SSLPorts.Analytics = int(ports.CbasSsl)

	return config
}

type ConfigParser struct{}

func (p ConfigParser) ParseTerseConfig(config *cbconfig.TerseConfigJson, sourceHostname string) (*ParsedConfig, error) {
	var out ParsedConfig
	out.RevID = int64(config.Rev)
	out.RevEpoch = int64(config.RevEpoch)

	lenNodes := len(config.Nodes)
	out.Nodes = make([]ParsedConfigNode, len(config.NodesExt))
	for nodeIdx, node := range config.NodesExt {
		nodeHostname := parseConfigHostname(node.Hostname, sourceHostname)

		var nodeOut ParsedConfigNode
		nodeOut.HasData = nodeIdx < lenNodes
		nodeOut.Addresses = parseConfigHostsInto(nodeHostname, node.Services)

		nodeOut.AltAddresses = make(map[string]ParsedConfigAddresses)
		for networkType, altAddrs := range node.AltAddresses {
			altHostname := parseConfigHostname(altAddrs.Hostname, nodeHostname)
			nodeOut.AltAddresses[networkType] = parseConfigHostsInto(altHostname, altAddrs.Ports)
		}

		out.Nodes[nodeIdx] = nodeOut
	}

	if config.Name != "" {
		out.BucketUUID = config.UUID
		out.BucketName = config.Name
		switch config.NodeLocator {
		case "ketama":
			out.BucketType = bktTypeMemcached
		case "vbucket":
			if len(config.VBucketServerMap.VBucketMap) == 0 {
				out.BucketType = bktTypeCouchbase
				out.VbucketMap = nil
				break
			}

			vbMap, err := NewVbucketMap(
				config.VBucketServerMap.VBucketMap,
				config.VBucketServerMap.NumReplicas)
			if err != nil {
				return nil, err
			}

			out.BucketType = bktTypeCouchbase
			out.VbucketMap = vbMap
		default:
			out.BucketType = bktTypeInvalid
		}
	}

	clusterCaps := config.ClusterCapabilities
	if clusterCaps != nil {
		ftsCaps := config.ClusterCapabilities["fts"]
		if ftsCaps != nil {
			out.Features.FtsVectorSearch = slices.Contains(ftsCaps, "vectorSearch")
		}
	}

	return &out, nil
}
