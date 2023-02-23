package gocbcorex

type NetworkTypeHeuristic struct{}

func (h NetworkTypeHeuristic) hostListContainsAddress(hosts *ParsedConfigAddresses, address string) bool {
	for _, configAddress := range hosts.NonSSL.Kv {
		if address == configAddress {
			return true
		}
	}
	for _, configAddress := range hosts.SSL.Kv {
		if address == configAddress {
			return true
		}
	}
	for _, configAddress := range hosts.NonSSL.Mgmt {
		if address == configAddress {
			return true
		}
	}
	for _, configAddress := range hosts.SSL.Mgmt {
		if address == configAddress {
			return true
		}
	}

	return false
}

func (h NetworkTypeHeuristic) Identify(config *ParsedConfig, address string) string {
	// if it matches one of the defaults use that, we check this first in case there is
	// overlap between the addresses and alt-addresses, we want to use the internal network
	if h.hostListContainsAddress(config.Addresses, address) {
		return "default"
	}

	// if it matches any of the alt-addresses, return that instead
	for networkType, altAddrs := range config.AlternateAddresses {
		if h.hostListContainsAddress(altAddrs, address) {
			return networkType
		}
	}

	// default to the default network if we can't heuristically guess
	return "default"
}
