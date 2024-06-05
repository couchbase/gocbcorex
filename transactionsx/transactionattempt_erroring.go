package transactionsx

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func mergeOperationFailedErrors(errs []*TransactionOperationFailedError) *TransactionOperationFailedError {
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

		aggCauses = append(aggCauses, tErr)

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

	return &TransactionOperationFailedError{
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

func (t *transactionAttempt) applyStateBits(stateBits uint32, errorBits uint32) {
	// This is a bit dirty, but its maximum going to do one retry per bit.
	for {
		oldStateBits := atomic.LoadUint32(&t.stateBits)
		newStateBits := oldStateBits | stateBits
		if errorBits > ((oldStateBits & transactionStateBitsMaskFinalError) >> transactionStateBitsPositionFinalError) {
			newStateBits = (newStateBits & transactionStateBitsMaskBits) | (errorBits << transactionStateBitsPositionFinalError)
		}

		t.logger.Info("Applying state bits",
			zap.Uint32("stateBits", stateBits),
			zap.Uint32("errorBits", errorBits),
			zap.Uint32("oldStateBits", oldStateBits),
			zap.Uint32("newStateBits", newStateBits))

		if atomic.CompareAndSwapUint32(&t.stateBits, oldStateBits, newStateBits) {
			break
		}
	}
}

// TODO(brett19): This is a hack for now
func (t *transactionAttempt) contextFailed(err error) *TransactionOperationFailedError {
	return t.operationFailed(operationFailedDef{
		Cerr:              classifyError(err),
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *transactionAttempt) operationFailed(def operationFailedDef) *TransactionOperationFailedError {
	t.logger.Info("Operation failed",
		zap.Bool("shouldNotRetry", def.ShouldNotRetry),
		zap.Bool("shouldNotRollback", def.ShouldNotRollback),
		zap.NamedError("cause", def.Cerr.Source),
		zap.Stringer("class", def.Cerr.Class),
		zap.Stringer("shouldRaise", def.Reason))

	err := &TransactionOperationFailedError{
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
	if def.ShouldNotRollback {
		stateBits |= transactionStateBitShouldNotRollback
	}
	if def.ShouldNotRetry {
		stateBits |= transactionStateBitShouldNotRetry
	}
	if def.Reason == TransactionErrorReasonTransactionExpired {
		stateBits |= transactionStateBitHasExpired
	}
	t.applyStateBits(stateBits, uint32(def.Reason))

	return err
}

func classifyHookError(err error) *classifiedError {
	// We currently have to classify the errors that are returned from the hooks, but
	// we should really just directly return the classifications and make the source
	// some special internal source showing it came from a hook...
	return classifyError(err)
}

func classifyError(err error) *classifiedError {
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

	if errors.Is(err, ErrTestAmbiguous) {
		ec = TransactionErrorClassFailAmbiguous
	} else if errors.Is(err, ErrTestTransient) {
		ec = TransactionErrorClassFailTransient
	} else if errors.Is(err, ErrTestHard) {
		ec = TransactionErrorClassFailHard
	} else if errors.Is(err, ErrTestOther) {
		ec = TransactionErrorClassFailOther
	}

	return &classifiedError{
		Source: err,
		Class:  ec,
	}
}
