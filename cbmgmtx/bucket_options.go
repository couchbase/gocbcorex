package cbmgmtx

type BucketType string

const (
	BucketTypeUnset     BucketType = ""
	BucketTypeCouchbase BucketType = "membase"
	BucketTypeMemcached BucketType = "memcached"
	BucketTypeEphemeral BucketType = "ephemeral"
)

type ConflictResolutionType string

const (
	ConflictResolutionTypeUnset          ConflictResolutionType = ""
	ConflictResolutionTypeTimestamp      ConflictResolutionType = "lww"
	ConflictResolutionTypeSequenceNumber ConflictResolutionType = "seqno"
	ConflictResolutionTypeCustom         ConflictResolutionType = "custom"
)

type EvictionPolicyType string

const (
	EvictionPolicyTypeUnset           EvictionPolicyType = ""
	EvictionPolicyTypeFull            EvictionPolicyType = "fullEviction"
	EvictionPolicyTypeValueOnly       EvictionPolicyType = "valueOnly"
	EvictionPolicyTypeNotRecentlyUsed EvictionPolicyType = "nruEviction"
	EvictionPolicyTypeNoEviction      EvictionPolicyType = "noEviction"
)

type CompressionMode string

const (
	CompressionModeUnset   CompressionMode = ""
	CompressionModeOff     CompressionMode = "off"
	CompressionModePassive CompressionMode = "passive"
	CompressionModeActive  CompressionMode = "active"
)

type StorageBackend string

const (
	StorageBackendUnset      StorageBackend = ""
	StorageBackendCouchstore StorageBackend = "couchstore"
	StorageBackendMagma      StorageBackend = "magma"
)

type DurabilityLevel string

const (
	DurabilityLevelUnset                      DurabilityLevel = ""
	DurabilityLevelNone                       DurabilityLevel = "none"
	DurabilityLevelMajority                   DurabilityLevel = "majority"
	DurabilityLevelMajorityAndPersistOnMaster DurabilityLevel = "majorityAndPersistActive"
	DurabilityLevelPersistToMajority          DurabilityLevel = "persistToMajority"
)
