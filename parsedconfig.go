package gocbcorex

type BucketType int

const (
	bktTypeNone      BucketType = -1
	bktTypeInvalid   BucketType = 0
	bktTypeCouchbase BucketType = iota
	bktTypeMemcached
)

type ParsedConfigServiceAddresses struct {
	Kv     []string
	KvData []string
	Mgmt   []string
	Views  []string
	Query  []string
	Search []string
}

type ParsedConfigAddresses struct {
	NonSSL ParsedConfigServiceAddresses
	SSL    ParsedConfigServiceAddresses
}

type ParsedConfig struct {
	RevID    int64
	RevEpoch int64

	BucketUUID string
	BucketName string
	BucketType BucketType
	VbucketMap *VbucketMap

	Addresses          *ParsedConfigAddresses
	AlternateAddresses map[string]*ParsedConfigAddresses
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

func (config *ParsedConfig) AddressesGroupForNetworkType(networkType string) *ParsedConfigAddresses {
	if networkType == "default" {
		return config.Addresses
	}

	addresses, ok := config.AlternateAddresses[networkType]
	if !ok {
		// if this network type does not exist, we return a blank list of endpoints
		return &ParsedConfigAddresses{}
	}

	return addresses
}
