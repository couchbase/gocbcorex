package transactionsx

import (
	"encoding/json"
)

type TxnStateJson string

const (
	TxnStateJsonUnknown    = TxnStateJson("")
	TxnStateJsonPending    = TxnStateJson("PENDING")
	TxnStateJsonCommitted  = TxnStateJson("COMMITTED")
	TxnStateJsonCompleted  = TxnStateJson("COMPLETED")
	TxnStateJsonAborted    = TxnStateJson("ABORTED")
	TxnStateJsonRolledBack = TxnStateJson("ROLLED_BACK")
)

type MutationTypeJson string

const (
	MutationTypeJsonInsert  = MutationTypeJson("insert")
	MutationTypeJsonReplace = MutationTypeJson("replace")
	MutationTypeJsonRemove  = MutationTypeJson("remove")
)

type DurabilityLevelJson string

const (
	DurabilityLevelJsonNone                       = DurabilityLevelJson("n")
	DurabilityLevelJsonMajority                   = DurabilityLevelJson("m")
	DurabilityLevelJsonMajorityAndPersistToActive = DurabilityLevelJson("pa")
	DurabilityLevelJsonPersistToMajority          = DurabilityLevelJson("pm")
)

type AtrMutationJson struct {
	BucketName     string `json:"bkt,omitempty"`
	ScopeName      string `json:"scp,omitempty"`
	CollectionName string `json:"col,omitempty"`
	DocID          string `json:"id,omitempty"`
}

type AtrAttemptJson struct {
	TransactionID   string       `json:"tid,omitempty"`
	ExpiryTimeNanos uint         `json:"exp,omitempty"`
	State           TxnStateJson `json:"st,omitempty"`

	PendingCAS    string `json:"tst,omitempty"`
	CommitCAS     string `json:"tsc,omitempty"`
	CompletedCAS  string `json:"tsco,omitempty"`
	AbortCAS      string `json:"tsrs,omitempty"`
	RolledBackCAS string `json:"tsrc,omitempty"`

	Inserts  []AtrMutationJson `json:"ins,omitempty"`
	Replaces []AtrMutationJson `json:"rep,omitempty"`
	Removes  []AtrMutationJson `json:"rem,omitempty"`

	DurabilityLevel DurabilityLevelJson `json:"d,omitempty"`

	ForwardCompat map[string][]ForwardCompatEntryJson `json:"fc,omitempty"`
}

type TxnXattrIdsJson struct {
	Transaction string `json:"txn,omitempty"`
	Attempt     string `json:"atmpt,omitempty"`
}

type TxnXattrAtrJson struct {
	DocID          string `json:"id,omitempty"`
	BucketName     string `json:"bkt,omitempty"`
	CollectionName string `json:"coll,omitempty"`
	ScopeName      string `json:"scp,omitempty"`
}

type TxnXattrOpJson struct {
	Type   MutationTypeJson `json:"type,omitempty"`
	Staged json.RawMessage  `json:"stgd,omitempty"`
	CRC32  string           `json:"crc32,omitempty"`
}

type TxnXattrRestoreJson struct {
	OriginalCAS string `json:"CAS,omitempty"`
	ExpiryTime  uint   `json:"exptime"`
	RevID       string `json:"revid,omitempty"`
}

type TxnXattrJson struct {
	ID            TxnXattrIdsJson                     `json:"id,omitempty"`
	ATR           TxnXattrAtrJson                     `json:"atr,omitempty"`
	Operation     TxnXattrOpJson                      `json:"op,omitempty"`
	Restore       *TxnXattrRestoreJson                `json:"restore,omitempty"`
	ForwardCompat map[string][]ForwardCompatEntryJson `json:"fc,omitempty"`
}

type TxnXattrDocMetaJson struct {
	Cas        string `json:"CAS"`
	RevID      string `json:"revid"`
	Expiration uint   `json:"exptime"`
	CRC32      string `json:"value_crc32c,omitempty"`
}

type ForwardCompatEntryJson struct {
	ProtocolVersion   string `json:"p,omitempty"`
	ProtocolExtension string `json:"e,omitempty"`
	Behaviour         string `json:"b,omitempty"`
	RetryInterval     int    `json:"ra,omitempty"`
}
