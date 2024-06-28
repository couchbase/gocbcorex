package transactionsx

import "errors"

// DurabilityLevel specifies the durability level to use for a mutation.
type DurabilityLevel int

const (
	// DurabilityLevelUnknown indicates to use the default level.
	DurabilityLevelUnknown = DurabilityLevel(0)

	// DurabilityLevelNone indicates that no durability is needed.
	DurabilityLevelNone = DurabilityLevel(1)

	// DurabilityLevelMajority indicates the operation must be replicated to the majority.
	DurabilityLevelMajority = DurabilityLevel(2)

	// DurabilityLevelMajorityAndPersistToActive indicates the operation must be replicated
	// to the majority and persisted to the active server.
	DurabilityLevelMajorityAndPersistToActive = DurabilityLevel(3)

	// DurabilityLevelPersistToMajority indicates the operation must be persisted to the active server.
	DurabilityLevelPersistToMajority = DurabilityLevel(4)
)

func durabilityLevelToString(level DurabilityLevel) string {
	switch level {
	case DurabilityLevelUnknown:
		return "UNSET"
	case DurabilityLevelNone:
		return "NONE"
	case DurabilityLevelMajority:
		return "MAJORITY"
	case DurabilityLevelMajorityAndPersistToActive:
		return "MAJORITY_AND_PERSIST_TO_ACTIVE"
	case DurabilityLevelPersistToMajority:
		return "PERSIST_TO_MAJORITY"
	}
	return ""
}

func durabilityLevelFromString(level string) (DurabilityLevel, error) {
	switch level {
	case "UNSET":
		return DurabilityLevelUnknown, nil
	case "NONE":
		return DurabilityLevelNone, nil
	case "MAJORITY":
		return DurabilityLevelMajority, nil
	case "MAJORITY_AND_PERSIST_TO_ACTIVE":
		return DurabilityLevelMajorityAndPersistToActive, nil
	case "PERSIST_TO_MAJORITY":
		return DurabilityLevelPersistToMajority, nil
	}
	return DurabilityLevelUnknown, errors.New("invalid durability level string")
}
