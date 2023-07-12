package gocbcore

// TransactionAttempt represents a singular attempt at executing a transaction.  A
// transaction may require multiple attempts before being successful.
type TransactionAttempt struct {
	State             TransactionAttemptState
	ID                string
	AtrID             []byte
	AtrBucketName     string
	AtrScopeName      string
	AtrCollectionName string

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool

	// Expired indicates whether this attempt expired during execution.
	Expired bool

	// PreExpiryAutoRollback indicates whether an auto-rollback occured
	// before the transaction was expired.
	PreExpiryAutoRollback bool
}

// TransactionResult represents the result of a transaction which was executed.
type TransactionResult struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// Attempts records all attempts that were performed when executing
	// this transaction.
	Attempts []TransactionAttempt

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool
}

// TransactionResourceUnitResult describes the number of resource units used by a transaction attempt.
// Internal: This should never be used and is not supported.
type TransactionResourceUnitResult struct {
	NumOps     uint32
	ReadUnits  uint32
	WriteUnits uint32
}
