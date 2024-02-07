package transactionsx

import (
	"github.com/couchbase/gocbcorex/memdx"
)

func transactionsDurabilityLevelToMemdx(durabilityLevel TransactionDurabilityLevel) memdx.DurabilityLevel {
	switch durabilityLevel {
	case TransactionDurabilityLevelNone:
		return memdx.DurabilityLevel(0)
	case TransactionDurabilityLevelMajority:
		return memdx.DurabilityLevelMajority
	case TransactionDurabilityLevelMajorityAndPersistToActive:
		return memdx.DurabilityLevelMajorityAndPersistToActive
	case TransactionDurabilityLevelPersistToMajority:
		return memdx.DurabilityLevelPersistToMajority
	case TransactionDurabilityLevelUnknown:
		panic("unexpected unset durability level")
	default:
		panic("unexpected durability level")
	}
}

func transactionsDurabilityLevelToShorthand(durabilityLevel TransactionDurabilityLevel) string {
	switch durabilityLevel {
	case TransactionDurabilityLevelNone:
		return "n"
	case TransactionDurabilityLevelMajority:
		return "m"
	case TransactionDurabilityLevelMajorityAndPersistToActive:
		return "pa"
	case TransactionDurabilityLevelPersistToMajority:
		return "pm"
	default:
		// If it's an unknown durability level, default to majority.
		return "m"
	}
}

// TODO(brett19): Use this when needed
/*
func transactionsDurabilityLevelFromShorthand(durabilityLevel string) TransactionDurabilityLevel {
	switch durabilityLevel {
	case "m":
		return TransactionDurabilityLevelMajority
	case "pa":
		return TransactionDurabilityLevelMajorityAndPersistToActive
	case "pm":
		return TransactionDurabilityLevelPersistToMajority
	default:
		// If there is no durability level present or it's set to none then we'll set to majority.
		return TransactionDurabilityLevelMajority
	}
}
*/
