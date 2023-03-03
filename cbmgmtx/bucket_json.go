package cbmgmtx

type bucketSettingsJson struct {
	Name        string `json:"name"`
	Controllers struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
	ReplicaIndex bool `json:"replicaIndex"`
	Quota        struct {
		RAM    uint64 `json:"ram"`
		RawRAM uint64 `json:"rawRAM"`
	} `json:"quota"`
	ReplicaNumber          uint32 `json:"replicaNumber"`
	BucketType             string `json:"bucketType"`
	ConflictResolutionType string `json:"conflictResolutionType"`
	EvictionPolicy         string `json:"evictionPolicy"`
	MaxTTL                 uint32 `json:"maxTTL"`
	CompressionMode        string `json:"compressionMode"`
	MinimumDurabilityLevel string `json:"durabilityMinLevel"`
	StorageBackend         string `json:"storageBackend"`
}
