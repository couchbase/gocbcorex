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

func durabilityLevelToMemdx(durabilityLevel DurabilityLevel) (memdx.DurabilityLevel, error) {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return memdx.DurabilityLevelNone, nil
	case DurabilityLevelMajority:
		return memdx.DurabilityLevelMajority, nil
	case DurabilityLevelMajorityAndPersistToActive:
		return memdx.DurabilityLevelMajorityAndPersistToActive, nil
	case DurabilityLevelPersistToMajority:
		return memdx.DurabilityLevelPersistToMajority, nil
	case DurabilityLevelUnknown:
		return memdx.DurabilityLevel(0), errors.New("cannot convert unknown durability level to memdx")
	default:
		return memdx.DurabilityLevel(0), errors.New("cannot convert unexpected durability level to memdx")
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

// txnStateFromJson maps the state recorded against an ATR entry onto its domain
// type.
//
// A state this client does not recognise is not an error.  A client running a
// later version of the protocol may write a state that did not exist when this
// one was built, and the protocol requires coping with that rather than refusing
// to look at the entry -- refusing means never cleaning it up, so one entry
// written by a newer client is leaked forever along with the documents it
// staged.  Such a state maps to TransactionAttemptStateUnknown, which callers
// must read as "leave this attempt's documents alone".
func txnStateFromJson(state TxnStateJson) TransactionAttemptState {
	switch state {
	case TxnStateJsonCommitted:
		return TransactionAttemptStateCommitted
	case TxnStateJsonCompleted:
		return TransactionAttemptStateCompleted
	case TxnStateJsonPending:
		return TransactionAttemptStatePending
	case TxnStateJsonAborted:
		return TransactionAttemptStateAborted
	case TxnStateJsonRolledBack:
		return TransactionAttemptStateRolledBack
	}

	return TransactionAttemptStateUnknown
}
