package gocbcorex

import (
	"fmt"

	"golang.org/x/exp/slices"
)

type NetworkTypeHeuristic struct{}

func (h NetworkTypeHeuristic) nodeContainsAddress(node ParsedConfigAddresses, address string) bool {
	matchAddresses := make([]string, 0, 10)
	matchAddresses = append(matchAddresses, fmt.Sprintf("%s:%d", node.Hostname, node.NonSSLPorts.Kv))
	matchAddresses = append(matchAddresses, fmt.Sprintf("%s:%d", node.Hostname, node.NonSSLPorts.Mgmt))
	matchAddresses = append(matchAddresses, fmt.Sprintf("%s:%d", node.Hostname, node.SSLPorts.Kv))
	matchAddresses = append(matchAddresses, fmt.Sprintf("%s:%d", node.Hostname, node.SSLPorts.Mgmt))
	return slices.Contains(matchAddresses, address)
}

func (h NetworkTypeHeuristic) Identify(config *ParsedConfig, address string) string {
	// if it matches one of the defaults use that, we check this first in case there is
	// overlap between the addresses and alt-addresses, we want to use the internal network
	for _, node := range config.Nodes {
		if h.nodeContainsAddress(node.Addresses, address) {
			return "default"
		}
	}

	// if it matches any of the alt-addresses, return that instead
	for _, node := range config.Nodes {
		for networkType, altAddrs := range node.AltAddresses {
			if h.nodeContainsAddress(altAddrs, address) {
				return networkType
			}
		}
	}

	// default to the default network if we can't heuristically guess
	return "default"
}
