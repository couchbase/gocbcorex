package transactionsx

import "fmt"

// TransactionLambdaResult represents the result of a transaction which was executed.
type TransactionLambdaResult struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// Attempts records all attempts that were performed when executing
	// this transaction.
	Attempts []*TransactionAttemptResult

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool
}

type TransactionLambdaError struct {
	Cause  error
	Result *TransactionLambdaResult
}

func (e *TransactionLambdaError) Error() string {
	return fmt.Sprintf("transaction lambda error (result: %+v): %s",
		e.Result,
		e.Cause)
}

func (e *TransactionLambdaError) Unwrap() error {
	return e.Cause
}
