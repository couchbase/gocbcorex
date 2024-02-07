package transactionsx

// TransactionAttemptResult represents a singular attempt at executing a transaction.  A
// transaction may require multiple attempts before being successful.
type TransactionAttemptResult struct {
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
	Attempts []TransactionAttemptResult

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool
}
