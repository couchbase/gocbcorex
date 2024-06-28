package memdx

// DurabilityLevel specifies the level to use for enhanced durability requirements.
type DurabilityLevel uint8

const (
	// DurabilityLevelNone indicates the operation does not require any durability guarantees.
	DurabilityLevelNone = DurabilityLevel(0)

	// DurabilityLevelMajority indicates the operation must be replicated to the majority.
	DurabilityLevelMajority = DurabilityLevel(1)

	// DurabilityLevelMajorityAndPersistToActive indicates the operation must be replicated
	// to the majority and persisted to the active server.
	DurabilityLevelMajorityAndPersistToActive = DurabilityLevel(2)

	// DurabilityLevelPersistToMajority indicates the operation must be persisted to the active server.
	DurabilityLevelPersistToMajority = DurabilityLevel(3)
)
