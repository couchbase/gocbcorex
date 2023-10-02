package cbconfig

type VBucketServerMapJson struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap,omitempty"`
}

type ConfigDDocsJson struct {
	URI string `json:"uri,omitempty"`
}

type FullNodeJson struct {
	CouchApiBase string                                  `json:"couchApiBase,omitempty"`
	Hostname     string                                  `json:"hostname,omitempty"`
	NodeUUID     string                                  `json:"nodeUUID,omitempty"`
	Ports        map[string]int                          `json:"ports,omitempty"`
	Services     []string                                `json:"services"`
	AltAddresses map[string]TerseExtNodeAltAddressesJson `json:"alternateAddresses,omitempty"`
}

type FullBucketConfigJson struct {
	Name                   string                `json:"name,omitempty"`
	NodeLocator            string                `json:"nodeLocator,omitempty"`
	UUID                   string                `json:"uuid,omitempty"`
	URI                    string                `json:"uri,omitempty"`
	StreamingURI           string                `json:"streamingUri,omitempty"`
	BucketCapabilitiesVer  string                `json:"bucketCapabilitiesVer,omitempty"`
	BucketCapabilities     []string              `json:"bucketCapabilities,omitempty"`
	CollectionsManifestUid string                `json:"collectionsManifestUid,omitempty"`
	DDocs                  *ConfigDDocsJson      `json:"ddocs,omitempty"`
	VBucketServerMap       *VBucketServerMapJson `json:"vBucketServerMap,omitempty"`
	Nodes                  []FullNodeJson        `json:"nodes,omitempty"`
}

type BucketNamesJson struct {
	BucketName string `json:"bucketName"`
	UUID       string `json:"uuid"`
}

type FullClusterConfigJson struct {
	Name        string            `json:"name,omitempty"`
	Nodes       []FullNodeJson    `json:"nodes,omitempty"`
	BucketNames []BucketNamesJson `json:"bucketNames"`
}

type ServerGroupGroupJson struct {
	Name  string         `json:"name,omitempty"`
	Nodes []FullNodeJson `json:"nodes"`
}

type ServerGroupConfigJson struct {
	Groups []ServerGroupGroupJson `json:"groups"`
}

type TerseNodePortsJson struct {
	Direct uint16 `json:"direct,omitempty"`
	Proxy  uint16 `json:"proxy,omitempty"`
}

type TerseNodeJson struct {
	CouchApiBase string              `json:"couchApiBase,omitempty"`
	Hostname     string              `json:"hostname,omitempty"`
	Ports        *TerseNodePortsJson `json:"ports,omitempty"`
}

type TerseExtNodePortsJson struct {
	Kv          uint16 `json:"kv,omitempty"`
	Capi        uint16 `json:"capi,omitempty"`
	Mgmt        uint16 `json:"mgmt,omitempty"`
	N1ql        uint16 `json:"n1ql,omitempty"`
	Fts         uint16 `json:"fts,omitempty"`
	Cbas        uint16 `json:"cbas,omitempty"`
	Eventing    uint16 `json:"eventingAdminPort,omitempty"`
	GSI         uint16 `json:"indexHttp,omitempty"`
	Backup      uint16 `json:"backupAPI,omitempty"`
	KvSsl       uint16 `json:"kvSSL,omitempty"`
	CapiSsl     uint16 `json:"capiSSL,omitempty"`
	MgmtSsl     uint16 `json:"mgmtSSL,omitempty"`
	N1qlSsl     uint16 `json:"n1qlSSL,omitempty"`
	FtsSsl      uint16 `json:"ftsSSL,omitempty"`
	CbasSsl     uint16 `json:"cbasSSL,omitempty"`
	EventingSsl uint16 `json:"eventingSSL,omitempty"`
	GSISsl      uint16 `json:"indexHttps,omitempty"`
	BackupSsl   uint16 `json:"backupAPIHTTPS,omitempty"`
}

type TerseExtNodeAltAddressesJson struct {
	Ports    *TerseExtNodePortsJson `json:"ports,omitempty"`
	Hostname string                 `json:"hostname,omitempty"`
}

type TerseExtNodeJson struct {
	Services     *TerseExtNodePortsJson                  `json:"services,omitempty"`
	ThisNode     bool                                    `json:"thisNode,omitempty"`
	Hostname     string                                  `json:"hostname,omitempty"`
	AltAddresses map[string]TerseExtNodeAltAddressesJson `json:"alternateAddresses,omitempty"`
}

type TerseConfigJson struct {
	Rev                    int                   `json:"rev,omitempty"`
	RevEpoch               int                   `json:"revEpoch,omitempty"`
	Name                   string                `json:"name,omitempty"`
	NodeLocator            string                `json:"nodeLocator,omitempty"`
	UUID                   string                `json:"uuid,omitempty"`
	URI                    string                `json:"uri,omitempty"`
	StreamingURI           string                `json:"streamingUri,omitempty"`
	BucketCapabilitiesVer  string                `json:"bucketCapabilitiesVer,omitempty"`
	BucketCapabilities     []string              `json:"bucketCapabilities,omitempty"`
	CollectionsManifestUid string                `json:"collectionsManifestUid,omitempty"`
	DDocs                  *ConfigDDocsJson      `json:"ddocs,omitempty"`
	VBucketServerMap       *VBucketServerMapJson `json:"vBucketServerMap,omitempty"`
	Nodes                  []TerseNodeJson       `json:"nodes,omitempty"`
	NodesExt               []TerseExtNodeJson    `json:"nodesExt,omitempty"`
	ClusterCapabilitiesVer []int                 `json:"clusterCapabilitiesVer"`
	ClusterCapabilities    map[string][]string   `json:"clusterCapabilities"`
}
