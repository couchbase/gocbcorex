package gocbcore

import (
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

func transactionsDurabilityLevelToMemd(durabilityLevel TransactionDurabilityLevel) memd.DurabilityLevel {
	switch durabilityLevel {
	case TransactionDurabilityLevelNone:
		return memd.DurabilityLevel(0)
	case TransactionDurabilityLevelMajority:
		return memd.DurabilityLevelMajority
	case TransactionDurabilityLevelMajorityAndPersistToActive:
		return memd.DurabilityLevelMajorityAndPersistOnMaster
	case TransactionDurabilityLevelPersistToMajority:
		return memd.DurabilityLevelPersistToMajority
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

func transactionsMutationTimeouts(opTimeout time.Duration, durability TransactionDurabilityLevel) (time.Time, time.Duration) {
	var deadline time.Time
	var duraTimeout time.Duration
	if opTimeout > 0 {
		deadline = time.Now().Add(opTimeout)
		if durability > TransactionDurabilityLevelNone {
			duraTimeout = opTimeout
		}
	}

	return deadline, duraTimeout
}
