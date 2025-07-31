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
	Status            string                                  `json:"status,omitempty"`
	ClusterMembership string                                  `json:"clusterMembership,omitempty"`
	ThisNode          bool                                    `json:"thisNode,omitempty"`
	CouchApiBase      string                                  `json:"couchApiBase,omitempty"`
	Hostname          string                                  `json:"hostname,omitempty"`
	NodeUUID          string                                  `json:"nodeUUID,omitempty"`
	OTPNode           string                                  `json:"otpNode,omitempty"`
	Ports             map[string]int                          `json:"ports,omitempty"`
	Services          []string                                `json:"services"`
	AltAddresses      map[string]TerseExtNodeAltAddressesJson `json:"alternateAddresses,omitempty"`
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

	Controllers struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
	ReplicaIndex bool `json:"replicaIndex"`
	Quota        struct {
		RAM    uint64 `json:"ram"`
		RawRAM uint64 `json:"rawRAM"`
	} `json:"quota"`
	ReplicaNumber                     uint32 `json:"replicaNumber"`
	BucketType                        string `json:"bucketType"`
	ConflictResolutionType            string `json:"conflictResolutionType"`
	EvictionPolicy                    string `json:"evictionPolicy"`
	MaxTTL                            uint32 `json:"maxTTL"`
	CompressionMode                   string `json:"compressionMode"`
	MinimumDurabilityLevel            string `json:"durabilityMinLevel"`
	StorageBackend                    string `json:"storageBackend"`
	HistoryRetentionCollectionDefault *bool  `json:"historyRetentionCollectionDefault"`
	HistoryRetentionBytes             uint64 `json:"historyRetentionBytes"`
	HistoryRetentionSeconds           uint32 `json:"historyRetentionSeconds"`
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

type CollectionManifestCollectionJson struct {
	UID     string `json:"uid"`
	Name    string `json:"name"`
	MaxTTL  int32  `json:"maxTTL,omitempty"`
	History bool   `json:"history,omitempty"`
}

type CollectionManifestScopeJson struct {
	UID         string                             `json:"uid"`
	Name        string                             `json:"name"`
	Collections []CollectionManifestCollectionJson `json:"collections,omitempty"`
}

type CollectionManifestJson struct {
	UID    string                        `json:"uid"`
	Scopes []CollectionManifestScopeJson `json:"scopes,omitempty"`
}
