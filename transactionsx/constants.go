package transactionsx

import "fmt"

// TransactionAttemptState represents the current State of a transaction
type TransactionAttemptState int

const (
	// TransactionAttemptStateNothingWritten indicates that nothing has been written yet.
	TransactionAttemptStateNothingWritten = TransactionAttemptState(1)

	// TransactionAttemptStatePending indicates that the transaction ATR has been written and
	// the transaction is currently pending.
	TransactionAttemptStatePending = TransactionAttemptState(2)

	// TransactionAttemptStateCommitting indicates that the transaction is now trying to become
	// committed, if we stay in this state, it implies ambiguity.
	TransactionAttemptStateCommitting = TransactionAttemptState(3)

	// TransactionAttemptStateCommitted indicates that the transaction is now logically committed
	// but the unstaging of documents is still underway.
	TransactionAttemptStateCommitted = TransactionAttemptState(4)

	// TransactionAttemptStateCompleted indicates that the transaction has been fully completed
	// and no longer has work to perform.
	TransactionAttemptStateCompleted = TransactionAttemptState(5)

	// TransactionAttemptStateAborted indicates that the transaction was aborted.
	TransactionAttemptStateAborted = TransactionAttemptState(6)

	// TransactionAttemptStateRolledBack indicates that the transaction was not committed and instead
	// was rolled back in its entirety.
	TransactionAttemptStateRolledBack = TransactionAttemptState(7)
)

func (state TransactionAttemptState) String() string {
	switch state {
	case TransactionAttemptStateNothingWritten:
		return "nothing_written"
	case TransactionAttemptStatePending:
		return "pending"
	case TransactionAttemptStateCommitting:
		return "committing"
	case TransactionAttemptStateCommitted:
		return "committed"
	case TransactionAttemptStateCompleted:
		return "completed"
	case TransactionAttemptStateAborted:
		return "aborted"
	case TransactionAttemptStateRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// TransactionErrorReason is the reason why a transaction should be failed.
// Internal: This should never be used and is not supported.
type TransactionErrorReason uint8

// NOTE: The errors within this section are critically ordered, as the order of
// precedence used when merging errors together is based on this.
const (
	// TransactionErrorReasonSuccess indicates the transaction succeeded and did not fail.
	TransactionErrorReasonSuccess TransactionErrorReason = TransactionErrorReason(0)

	// TransactionErrorReasonTransactionFailed indicates the transaction should be failed because it failed.
	TransactionErrorReasonTransactionFailed = TransactionErrorReason(1)

	// TransactionErrorReasonTransactionExpired indicates the transaction should be failed because it expired.
	TransactionErrorReasonTransactionExpired = TransactionErrorReason(2)

	// TransactionErrorReasonTransactionCommitAmbiguous indicates the transaction should be failed and the commit was ambiguous.
	TransactionErrorReasonTransactionCommitAmbiguous = TransactionErrorReason(3)

	// TransactionErrorReasonTransactionFailedPostCommit indicates the transaction should be failed because it failed post commit.
	TransactionErrorReasonTransactionFailedPostCommit = TransactionErrorReason(4)
)

func (reason TransactionErrorReason) String() string {
	switch reason {
	case TransactionErrorReasonTransactionFailed:
		return "failed"
	case TransactionErrorReasonTransactionExpired:
		return "expired"
	case TransactionErrorReasonTransactionCommitAmbiguous:
		return "commit_ambiguous"
	case TransactionErrorReasonTransactionFailedPostCommit:
		return "failed_post_commit"
	default:
		return fmt.Sprintf("unknown:%d", reason)
	}
}

// TransactionErrorClass describes the reason that a transaction error occurred.
// Internal: This should never be used and is not supported.
type TransactionErrorClass uint8

const (
	// TransactionErrorClassFailOther indicates an error occurred because it did not fit into any other reason.
	TransactionErrorClassFailOther TransactionErrorClass = iota

	// TransactionErrorClassFailTransient indicates an error occurred because of a transient reason.
	TransactionErrorClassFailTransient

	// TransactionErrorClassFailDocNotFound indicates an error occurred because of a document not found.
	TransactionErrorClassFailDocNotFound

	// TransactionErrorClassFailDocAlreadyExists indicates an error occurred because a document already exists.
	TransactionErrorClassFailDocAlreadyExists

	// TransactionErrorClassFailPathNotFound indicates an error occurred because a path was not found.
	TransactionErrorClassFailPathNotFound

	// TransactionErrorClassFailPathAlreadyExists indicates an error occurred because a path already exists.
	TransactionErrorClassFailPathAlreadyExists

	// TransactionErrorClassFailWriteWriteConflict indicates an error occurred because of a write write conflict.
	TransactionErrorClassFailWriteWriteConflict

	// TransactionErrorClassFailCasMismatch indicates an error occurred because of a cas mismatch.
	TransactionErrorClassFailCasMismatch

	// TransactionErrorClassFailHard indicates an error occurred because of a hard error.
	TransactionErrorClassFailHard

	// TransactionErrorClassFailAmbiguous indicates an error occurred leaving the transaction in an ambiguous way.
	TransactionErrorClassFailAmbiguous

	// TransactionErrorClassFailExpiry indicates an error occurred because the transaction expired.
	TransactionErrorClassFailExpiry

	// TransactionErrorClassFailOutOfSpace indicates an error occurred because the ATR is full.
	TransactionErrorClassFailOutOfSpace
)

func (class TransactionErrorClass) String() string {
	switch class {
	case TransactionErrorClassFailOther:
		return "other"
	case TransactionErrorClassFailTransient:
		return "transient"
	case TransactionErrorClassFailDocNotFound:
		return "doc_not_found"
	case TransactionErrorClassFailDocAlreadyExists:
		return "doc_already_exists"
	case TransactionErrorClassFailPathNotFound:
		return "path_not_found"
	case TransactionErrorClassFailPathAlreadyExists:
		return "path_already_exists"
	case TransactionErrorClassFailWriteWriteConflict:
		return "write_write_conflict"
	case TransactionErrorClassFailCasMismatch:
		return "cas_mismatch"
	case TransactionErrorClassFailHard:
		return "hard"
	case TransactionErrorClassFailAmbiguous:
		return "ambiguous"
	case TransactionErrorClassFailExpiry:
		return "expiry"
	case TransactionErrorClassFailOutOfSpace:
		return "out_of_space"
	default:
		return fmt.Sprintf("unknown:%d", class)
	}
}

const (
	transactionStateBitShouldNotCommit       = 1 << 0
	transactionStateBitShouldNotRollback     = 1 << 1
	transactionStateBitShouldNotRetry        = 1 << 2
	transactionStateBitHasExpired            = 1 << 3
	transactionStateBitPreExpiryAutoRollback = 1 << 4

	transactionStateBitsFinalErrorPos  = 5
	transactionStateBitsFinalErrorBits = 0b111
)
