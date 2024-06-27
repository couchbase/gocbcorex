package transactionsx

import (
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/pkg/errors"
)

func forwardCompatFromJson(fc map[string][]jsonForwardCompatibilityEntry) map[string][]TransactionForwardCompatibilityEntry {
	if fc == nil {
		return nil
	}
	forwardCompat := make(map[string][]TransactionForwardCompatibilityEntry)

	for k, entries := range fc {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]TransactionForwardCompatibilityEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = TransactionForwardCompatibilityEntry(entry)
		}
	}

	return forwardCompat
}

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

func transactionsDurabilityLevelToJson(durabilityLevel TransactionDurabilityLevel) jsonDurabilityLevel {
	switch durabilityLevel {
	case TransactionDurabilityLevelNone:
		return jsonDurabilityLevelNone
	case TransactionDurabilityLevelMajority:
		return jsonDurabilityLevelMajority
	case TransactionDurabilityLevelMajorityAndPersistToActive:
		return jsonDurabilityLevelMajorityAndPersistToActive
	case TransactionDurabilityLevelPersistToMajority:
		return jsonDurabilityLevelPersistToMajority
	default:
		// If it's an unknown durability level, default to majority.
		return jsonDurabilityLevelMajority
	}
}

func transactionsDurabilityLevelFromJson(durabilityLevel jsonDurabilityLevel) TransactionDurabilityLevel {
	switch durabilityLevel {
	case jsonDurabilityLevelNone:
		return TransactionDurabilityLevelNone
	case jsonDurabilityLevelMajority:
		return TransactionDurabilityLevelMajority
	case jsonDurabilityLevelMajorityAndPersistToActive:
		return TransactionDurabilityLevelMajorityAndPersistToActive
	case jsonDurabilityLevelPersistToMajority:
		return TransactionDurabilityLevelPersistToMajority
	default:
		// If there is no durability level present or it's set to none then we'll set to majority.
		return TransactionDurabilityLevelMajority
	}
}

func transactionsStateFromJson(state jsonAtrState) (TransactionAttemptState, error) {
	switch state {
	case jsonAtrStateCommitted:
		return TransactionAttemptStateCommitted, nil
	case jsonAtrStateCompleted:
		return TransactionAttemptStateCompleted, nil
	case jsonAtrStatePending:
		return TransactionAttemptStatePending, nil
	case jsonAtrStateAborted:
		return TransactionAttemptStateAborted, nil
	case jsonAtrStateRolledBack:
		return TransactionAttemptStateRolledBack, nil
	}

	return TransactionAttemptState(0), errors.New("unexpected transaction state value")
}
