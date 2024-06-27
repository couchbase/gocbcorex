package transactionsx

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func mergeOperationFailedErrors(errs []*transactionOperationStatus) *transactionOperationStatus {
	if len(errs) == 0 {
		return nil
	}

	if len(errs) == 1 {
		return errs[0]
	}

	shouldNotRetry := false
	shouldNotRollback := false
	aggCauses := aggregateError{}
	shouldRaise := TransactionErrorReasonTransactionFailed

	for errIdx := 0; errIdx < len(errs); errIdx++ {
		tErr := errs[errIdx]

		aggCauses = append(aggCauses, tErr.Err())

		if tErr.shouldNotRetry {
			shouldNotRetry = true
		}
		if tErr.shouldNotRollback {
			shouldNotRollback = true
		}
		if tErr.shouldRaise > shouldRaise {
			shouldRaise = tErr.shouldRaise
		}
	}

	return &transactionOperationStatus{
		shouldNotRetry:    shouldNotRetry,
		shouldNotRollback: shouldNotRollback,
		errorCause:        aggCauses,
		shouldRaise:       shouldRaise,
		errorClass:        TransactionErrorClassFailOther,
	}
}

type operationFailedDef struct {
	Cerr              *classifiedError
	ShouldNotRetry    bool
	ShouldNotRollback bool
	CanStillCommit    bool
	Reason            TransactionErrorReason
}

func (t *TransactionAttempt) applyStateBits(stateBits uint32, errorReason TransactionErrorReason) {
	errorBits := uint32(errorReason)

	// if the error bits take too much space to store, we emit an error
	// log message and saturate the error bits. note that a fully saturated
	// error reason is not considered a valid possible value (>=).
	if errorBits >= transactionStateBitsFinalErrorBits {
		t.logger.DPanic("requested error reason was too large to store")
		errorBits = transactionStateBitsFinalErrorBits
	}

	validStateBits := uint32(1<<transactionStateBitsFinalErrorPos) - 1
	if stateBits > validStateBits {
		// if we are trying to set state bits that overlap the error bits
		// we should log an error and then truncate the state bits.
		t.logger.DPanic("requested state bits were too large to store")
		stateBits = stateBits & validStateBits
	}

	// this is a bit dirty, but its maximum going to do one retry per bit
	// since state bits can only be set, not unset.  we only need to retry
	// if we are trying to set a bit that wasn't set before, and would only
	// conflict if someone else set it, meaning no more tries to set that
	// particular bit are needed.
	for {
		oldStateBits := atomic.LoadUint32(&t.stateBits)
		oldErrorBits := (oldStateBits & transactionStateBitsFinalErrorBits) >> transactionStateBitsFinalErrorPos

		newStateBits := oldStateBits
		newStateBits = newStateBits | stateBits
		if errorBits > oldErrorBits {
			newStateBits = newStateBits | (errorBits << transactionStateBitsFinalErrorPos)
		}

		if oldStateBits == newStateBits {
			// if we didn't actually change the state bits, then we don't need to do anything.
			t.logger.Debug("state bits did not change, not applying")
			break
		}

		t.logger.Info("attempting to apply state bits",
			zap.Uint32("stateBits", stateBits),
			zap.Uint32("errorBits", errorBits),
			zap.Uint32("oldStateBits", oldStateBits),
			zap.Uint32("newStateBits", newStateBits))

		if atomic.CompareAndSwapUint32(&t.stateBits, oldStateBits, newStateBits) {
			t.logger.Debug("failed to apply state bits, retrying")
			break
		}
	}
}

func (t *TransactionAttempt) contextFailed(err error) *transactionOperationStatus {
	return t.operationFailed(operationFailedDef{
		Cerr:              classifyError(err),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) operationFailed(def operationFailedDef) *transactionOperationStatus {
	t.logger.Info("operation failed",
		zap.Bool("canStillCommit", def.CanStillCommit),
		zap.Bool("shouldNotRetry", def.ShouldNotRetry),
		zap.Bool("shouldNotRollback", def.ShouldNotRollback),
		zap.NamedError("cause", def.Cerr.Source),
		zap.Stringer("class", def.Cerr.Class),
		zap.Stringer("shouldRaise", def.Reason))

	err := &transactionOperationStatus{
		canStillCommit:    def.CanStillCommit,
		shouldNotRetry:    def.ShouldNotRetry,
		shouldNotRollback: def.ShouldNotRollback,
		errorCause:        def.Cerr.Source,
		errorClass:        def.Cerr.Class,
		shouldRaise:       def.Reason,
	}

	stateBits := uint32(0)
	if !def.CanStillCommit {
		stateBits |= transactionStateBitShouldNotCommit
	}
	if def.ShouldNotRetry {
		stateBits |= transactionStateBitShouldNotRetry
	}
	if def.ShouldNotRollback {
		stateBits |= transactionStateBitShouldNotRollback
	}
	if def.Reason == TransactionErrorReasonTransactionExpired {
		stateBits |= transactionStateBitHasExpired
	}
	t.applyStateBits(stateBits, def.Reason)

	return err
}

func classifyHookError(err error) *classifiedError {
	// We currently have to classify the errors that are returned from the hooks, but
	// we should really just directly return the classifications and make the source
	// some special internal source showing it came from a hook...
	return classifyError(err)
}

func classifyError(err error) *classifiedError {
	var ece *ErrorClassError
	if errors.As(err, &ece) {
		return &classifiedError{
			Source: err,
			Class:  ece.Class,
		}
	}

	ec := TransactionErrorClassFailOther
	if errors.Is(err, memdx.ErrDocNotFound) {
		ec = TransactionErrorClassFailDocNotFound
	} else if errors.Is(err, memdx.ErrCasMismatch) {
		ec = TransactionErrorClassFailCasMismatch
	} else if errors.Is(err, memdx.ErrDocNotFound) {
		ec = TransactionErrorClassFailDocNotFound
	} else if errors.Is(err, memdx.ErrDocExists) {
		ec = TransactionErrorClassFailDocAlreadyExists
	} else if errors.Is(err, memdx.ErrSubDocPathExists) {
		ec = TransactionErrorClassFailPathAlreadyExists
	} else if errors.Is(err, memdx.ErrSubDocPathNotFound) {
		ec = TransactionErrorClassFailPathNotFound
	} else if errors.Is(err, memdx.ErrCasMismatch) {
		ec = TransactionErrorClassFailCasMismatch
	} else if errors.Is(err, memdx.ErrValueTooLarge) {
		ec = TransactionErrorClassFailOutOfSpace
	} else if errors.Is(err, memdx.ErrTmpFail) {
		ec = TransactionErrorClassFailTransient
	} else if errors.Is(err, memdx.ErrSyncWriteAmbiguous) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		ec = TransactionErrorClassFailAmbiguous
	}

	if errors.Is(err, ErrAttemptExpired) {
		ec = TransactionErrorClassFailExpiry
	}

	return &classifiedError{
		Source: err,
		Class:  ec,
	}
}
