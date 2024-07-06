package transactionsx

import (
	"strconv"

	"github.com/couchbase/gocbcorex/cbqueryx"
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

func durabilityLevelToQueryx(durabilityLevel DurabilityLevel) (cbqueryx.DurabilityLevel, error) {
	switch durabilityLevel {
	case DurabilityLevelNone:
		return cbqueryx.DurabilityLevelNone, nil
	case DurabilityLevelMajority:
		return cbqueryx.DurabilityLevelMajority, nil
	case DurabilityLevelMajorityAndPersistToActive:
		return cbqueryx.DurabilityLevelMajorityAndPersistToActive, nil
	case DurabilityLevelPersistToMajority:
		return cbqueryx.DurabilityLevelPersistToMajority, nil
	case DurabilityLevelUnknown:
		return cbqueryx.DurabilityLevel(""), errors.New("cannot convert unknown durability level to cbqueryx")
	default:
		return cbqueryx.DurabilityLevel(""), errors.New("cannot convert unexpected durability level to cbqueryx")
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

func scasToCas(scas string) (uint64, error) {
	i, err := strconv.ParseUint(scas, 10, 64)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func casToScas(cas uint64) string {
	return strconv.FormatUint(uint64(cas), 10)
}
