package transactionsx

import (
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/pkg/errors"
)

func forwardCompatFromJson(fc map[string][]ForwardCompatEntryJson) map[string][]ForwardCompatEntry {
	if fc == nil {
		return nil
	}
	forwardCompat := make(map[string][]ForwardCompatEntry)

	for k, entries := range fc {
		if _, ok := forwardCompat[k]; !ok {
			forwardCompat[k] = make([]ForwardCompatEntry, len(entries))
		}

		for i, entry := range entries {
			forwardCompat[k][i] = ForwardCompatEntry(entry)
		}
	}

	return forwardCompat
}

func durabilityLevelToMemdx(durabilityLevel DurabilityLevel) memdx.DurabilityLevel {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return memdx.DurabilityLevel(0)
	case DurabilityLevelMajority:
		return memdx.DurabilityLevelMajority
	case DurabilityLevelMajorityAndPersistToActive:
		return memdx.DurabilityLevelMajorityAndPersistToActive
	case DurabilityLevelPersistToMajority:
		return memdx.DurabilityLevelPersistToMajority
	case DurabilityLevelUnknown:
		panic("unexpected unset durability level")
	default:
		panic("unexpected durability level")
	}
}

func durabilityLevelToJson(durabilityLevel DurabilityLevel) DurabilityLevelJson {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return DurabilityLevelJsonNone
	case DurabilityLevelMajority:
		return DurabilityLevelJsonMajority
	case DurabilityLevelMajorityAndPersistToActive:
		return DurabilityLevelJsonMajorityAndPersistToActive
	case DurabilityLevelPersistToMajority:
		return DurabilityLevelJsonPersistToMajority
	default:
		// If it's an unknown durability level, default to majority.
		return DurabilityLevelJsonMajority
	}
}

func durabilityLevelFromJson(durabilityLevel DurabilityLevelJson) DurabilityLevel {
	switch durabilityLevel {
	case DurabilityLevelJsonNone:
		return DurabilityLevelNone
	case DurabilityLevelJsonMajority:
		return DurabilityLevelMajority
	case DurabilityLevelJsonMajorityAndPersistToActive:
		return DurabilityLevelMajorityAndPersistToActive
	case DurabilityLevelJsonPersistToMajority:
		return DurabilityLevelPersistToMajority
	default:
		// If there is no durability level present or it's set to none then we'll set to majority.
		return DurabilityLevelMajority
	}
}

func txnStateFromJson(state TxnStateJson) (TransactionAttemptState, error) {
	switch state {
	case TxnStateJsonCommitted:
		return TransactionAttemptStateCommitted, nil
	case TxnStateJsonCompleted:
		return TransactionAttemptStateCompleted, nil
	case TxnStateJsonPending:
		return TransactionAttemptStatePending, nil
	case TxnStateJsonAborted:
		return TransactionAttemptStateAborted, nil
	case TxnStateJsonRolledBack:
		return TransactionAttemptStateRolledBack, nil
	}

	return TransactionAttemptState(0), errors.New("unexpected transaction state value")
}
