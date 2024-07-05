package gocbcorex

import "fmt"

type BucketType int

const (
	bktTypeNone      BucketType = -1
	bktTypeInvalid   BucketType = 0
	bktTypeCouchbase BucketType = iota
	bktTypeMemcached
)

type ParsedConfigServicePorts struct {
	Kv        int
	Mgmt      int
	Views     int
	Query     int
	Search    int
	Analytics int
}

type ParsedConfigAddresses struct {
	Hostname    string
	NonSSLPorts ParsedConfigServicePorts
	SSLPorts    ParsedConfigServicePorts
}

type ParsedConfigNode struct {
	HasData      bool
	Addresses    ParsedConfigAddresses
	AltAddresses map[string]ParsedConfigAddresses
}

type ParsedConfigFeatures struct {
	FtsVectorSearch bool
}

type ParsedConfig struct {
	RevID    int64
	RevEpoch int64

	BucketUUID string
	BucketName string
	BucketType BucketType
	VbucketMap *VbucketMap

	Nodes []ParsedConfigNode

	Features ParsedConfigFeatures
}

func (config *ParsedConfig) IsVersioned() bool {
	return config.RevEpoch > 0 || config.RevID > 0
}

func (config *ParsedConfig) Compare(oconfig *ParsedConfig) int {
	if config.RevEpoch < oconfig.RevEpoch {
		// this config is an older epoch
		return -2
	} else if config.RevEpoch > oconfig.RevEpoch {
		// this config is a newer epoch
		return +2
	}

	if config.RevID < oconfig.RevID {
		// this config is an older config
		return -1
	} else if config.RevID > oconfig.RevID {
		// this config is a newer config
		return +1
	}

	return 0
}

type NetworkConfigNode struct {
	NodeID      string
	Hostname    string
	HasData     bool
	NonSSLPorts ParsedConfigServicePorts
	SSLPorts    ParsedConfigServicePorts
}

type NetworkConfig struct {
	Nodes []NetworkConfigNode
}

func (config *ParsedConfig) AddressesGroupForNetworkType(networkType string) *NetworkConfig {
	nodes := make([]NetworkConfigNode, 0, len(config.Nodes))

	for _, node := range config.Nodes {
		nodeInfo := NetworkConfigNode{
			NodeID:  fmt.Sprintf("ep-%s-%d", node.Addresses.Hostname, node.Addresses.NonSSLPorts.Mgmt),
			HasData: node.HasData,
		}

		if networkType == "default" {
			nodeInfo.Hostname = node.Addresses.Hostname
			nodeInfo.NonSSLPorts = node.Addresses.NonSSLPorts
			nodeInfo.SSLPorts = node.Addresses.SSLPorts
		} else {
			if altInfo, ok := node.AltAddresses[networkType]; ok {
				nodeInfo.Hostname = altInfo.Hostname
				nodeInfo.NonSSLPorts = altInfo.NonSSLPorts
				nodeInfo.SSLPorts = altInfo.SSLPorts
			} else {
				// explicitly indicate these fields will be blank
				nodeInfo.Hostname = ""
				nodeInfo.NonSSLPorts = ParsedConfigServicePorts{}
				nodeInfo.SSLPorts = ParsedConfigServicePorts{}
			}
		}

		nodes = append(nodes, nodeInfo)
	}

	return &NetworkConfig{Nodes: nodes}
}
