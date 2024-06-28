package transactionsx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

func transactionHasExpired(expiryTime time.Time) bool {
	return time.Now().After(expiryTime)
}

func (t *TransactionAttempt) beginOpLocked() {
	t.numPendingOps++
}

func (t *TransactionAttempt) endOp() {
	t.lock.Lock()

	t.numPendingOps--

	if t.numPendingOps > 0 {
		t.lock.Unlock()
		return
	}

	opsWaitCh := t.opsWaitCh
	t.opsWaitCh = nil

	t.lock.Unlock()

	if opsWaitCh != nil {
		close(opsWaitCh)
	}
}

func (t *TransactionAttempt) waitForOpsLocked(ctx context.Context) *transactionOperationStatus {
	for {
		if t.numPendingOps == 0 {
			return nil
		}

		if t.opsWaitCh == nil {
			t.opsWaitCh = make(chan struct{})
		}

		opsWaitCh := t.opsWaitCh

		t.lock.Unlock()

		select {
		case <-opsWaitCh:
			// ops wait channel closed, lets loop around and try again
		case <-ctx.Done():
			// context closed, let's bail...
			t.lock.Lock()

			cerr := classifyError(ctx.Err())
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
		}

		t.lock.Lock()
	}
}

func (t *TransactionAttempt) checkCanPerformOpLocked() *transactionOperationStatus {
	switch t.state {
	case TransactionAttemptStateNothingWritten:
		fallthrough
	case TransactionAttemptStatePending:
		// Good to continue
	case TransactionAttemptStateCommitting:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction is ambiguously committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TransactionAttemptStateCommitted:
		fallthrough
	case TransactionAttemptStateCompleted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TransactionAttemptStateAborted:
		fallthrough
	case TransactionAttemptStateRolledBack:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already aborted")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, fmt.Sprintf("invalid transaction state: %v", t.state))),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotCommit) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrPreviousOperationFailed, "previous operation prevents further operations")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *TransactionAttempt) checkCanCommitRollbackLocked() *transactionOperationStatus {
	switch t.state {
	case TransactionAttemptStateNothingWritten:
		fallthrough
	case TransactionAttemptStatePending:
		// Good to continue
	case TransactionAttemptStateCommitting:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction is ambiguously committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TransactionAttemptStateCommitted:
		fallthrough
	case TransactionAttemptStateCompleted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already committed")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TransactionAttemptStateAborted:
		fallthrough
	case TransactionAttemptStateRolledBack:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already aborted")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, fmt.Sprintf("invalid transaction state: %v", t.state))),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *TransactionAttempt) checkCanCommitLocked() *transactionOperationStatus {
	err := t.checkCanCommitRollbackLocked()
	if err != nil {
		return err
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotCommit) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrPreviousOperationFailed, "previous operation prevents commit")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *TransactionAttempt) checkCanRollbackLocked() *transactionOperationStatus {
	err := t.checkCanCommitRollbackLocked()
	if err != nil {
		return err
	}

	stateBits := atomic.LoadUint32(&t.stateBits)
	if (stateBits & transactionStateBitShouldNotRollback) != 0 {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrPreviousOperationFailed, "previous operation prevents rollback")),
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	return nil
}

func (t *TransactionAttempt) setExpiryOvertimeAtomic() {
	t.logger.Info("entering expiry overtime")

	t.applyStateBits(transactionStateBitHasExpired, 0)
}

func (t *TransactionAttempt) isExpiryOvertimeAtomic() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitHasExpired) != 0
}

func (t *TransactionAttempt) checkExpiredAtomic(ctx context.Context, stage string, id []byte, proceedInOvertime bool) *classifiedError {
	if proceedInOvertime && t.isExpiryOvertimeAtomic() {
		return nil
	}

	expired, err := t.hooks.HasExpiredClientSideHook(ctx, stage, id)
	if err != nil {
		return classifyError(wrapError(err, "HasExpired hook returned an unexpected error"))
	}

	if expired {
		return classifyError(wrapError(ErrAttemptExpired, "a hook has marked this attempt expired"))
	} else if transactionHasExpired(t.expiryTime) {
		return classifyError(wrapError(ErrAttemptExpired, "the expiry for the attempt was reached"))
	}

	return nil
}

func (t *TransactionAttempt) confirmATRPending(
	ctx context.Context,
	firstAgent *gocbcorex.Agent,
	firstOboUser string,
	firstKey []byte,
) *transactionOperationStatus {
	t.lock.Lock()

	for {
		if t.state != TransactionAttemptStateNothingWritten {
			t.lock.Unlock()
			return nil
		}

		otherAtrWaitCh := t.atrWaitCh
		if otherAtrWaitCh != nil {
			t.lock.Unlock()

			select {
			case <-otherAtrWaitCh:
				// wait complete, loop around
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}

			t.lock.Lock()

			continue
		}

		break
	}

	atrWaitCh := make(chan struct{})
	t.atrWaitCh = atrWaitCh

	t.lock.Unlock()

	err := t.selectAtrExclusive(
		ctx,
		firstAgent,
		firstOboUser,
		firstKey)
	if err != nil {
		t.lock.Lock()
		t.atrWaitCh = nil
		t.lock.Unlock()
		close(atrWaitCh)

		return err
	}

	err = t.setATRPendingExclusive(ctx)
	if err != nil {
		t.lock.Lock()
		t.atrWaitCh = nil
		t.lock.Unlock()
		close(atrWaitCh)

		return err
	}

	t.lock.Lock()

	t.state = TransactionAttemptStatePending
	t.atrWaitCh = nil

	t.lock.Unlock()
	close(atrWaitCh)

	return nil
}

func (t *TransactionAttempt) getStagedMutationLocked(
	bucketName, scopeName, collectionName string, key []byte,
) (int, *stagedMutation) {
	for i, mutation := range t.stagedMutations {
		if mutation.Agent.BucketName() == bucketName &&
			mutation.ScopeName == scopeName &&
			mutation.CollectionName == collectionName &&
			bytes.Equal(mutation.Key, key) {
			return i, mutation
		}
	}

	return -1, nil
}

func (t *TransactionAttempt) removeStagedMutation(
	bucketName, scopeName, collectionName string, key []byte,
) {
	t.lock.Lock()
	defer t.lock.Unlock()

	mutIdx, _ := t.getStagedMutationLocked(bucketName, scopeName, collectionName, key)
	if mutIdx >= 0 {
		// Not finding the item should be basically impossible, but we wrap it just in case...
		t.stagedMutations = append(t.stagedMutations[:mutIdx], t.stagedMutations[mutIdx+1:]...)
	}
}

func (t *TransactionAttempt) recordStagedMutation(
	stagedInfo *stagedMutation,
) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.enableMutationCaching {
		stagedInfo.Staged = nil
	}

	mutIdx, _ := t.getStagedMutationLocked(
		stagedInfo.Agent.BucketName(),
		stagedInfo.ScopeName,
		stagedInfo.CollectionName,
		stagedInfo.Key)
	if mutIdx >= 0 {
		t.stagedMutations[mutIdx] = stagedInfo
	} else {
		t.stagedMutations = append(t.stagedMutations, stagedInfo)
	}
}

func (t *TransactionAttempt) checkForwardCompatability(
	ctx context.Context,
	key []byte,
	bucket, scope, collection string,
	stage forwardCompatStage,
	fc map[string][]ForwardCompatEntry,
	forceNonFatal bool,
) *transactionOperationStatus {
	t.logger.Info("checking forward compatibility")

	isCompat, shouldRetry, retryWait, err := checkForwardCompatability(stage, fc)
	if err != nil {
		t.logger.Info("forward compatability error", zap.Error(err))

		return t.operationFailed(operationFailedDef{
			Cerr:              classifyError(err),
			CanStillCommit:    forceNonFatal,
			ShouldNotRetry:    false,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	if isCompat {
		return nil
	}

	if !shouldRetry {
		t.logger.Info("forward compatability failed - incompatible, no retry")

		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(forwardCompatError{
				BucketName:     bucket,
				ScopeName:      scope,
				CollectionName: collection,
				DocumentKey:    key,
			}),
			CanStillCommit:    forceNonFatal,
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	t.logger.Info("forward compatability failed - incompatible, should retry",
		zap.Duration("retryWait", retryWait))

	if retryWait > 0 {
		select {
		case <-time.After(retryWait):
			// continue below
		case <-ctx.Done():
			return t.contextFailed(ctx.Err())
		}
	}

	return t.operationFailed(operationFailedDef{
		Cerr: classifyError(forwardCompatError{
			BucketName:     bucket,
			ScopeName:      scope,
			CollectionName: collection,
			DocumentKey:    key,
		}),
		CanStillCommit:    forceNonFatal,
		ShouldNotRetry:    false,
		ShouldNotRollback: false,
		Reason:            TransactionErrorReasonTransactionFailed,
	})
}

func (t *TransactionAttempt) getTxnState(
	ctx context.Context,
	srcBucketName string,
	srcScopeName string,
	srcCollectionName string,
	srcDocID []byte,
	atrBucketName string,
	atrScopeName string,
	atrCollectionName string,
	atrDocID string,
	attemptID string,
	forceNonFatal bool,
) (*AtrAttemptJson, time.Time, *transactionOperationStatus) {
	ecCb := func(res *AtrAttemptJson, txnExp time.Time, cerr *classifiedError) (*AtrAttemptJson, time.Time, *transactionOperationStatus) {
		if cerr == nil {
			return res, txnExp, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailPathNotFound:
			t.logger.Info("attempt entry not found")

			// If the path is not found, we just return as if there was no
			// entry data available for that atr entry.
			return nil, time.Time{}, nil
		case TransactionErrorClassFailDocNotFound:
			t.logger.Info("atr doc not found")

			// If the ATR is not found, we just return as if there was no
			// entry data available for that atr entry.
			return nil, time.Time{}, nil
		default:
			return nil, time.Time{}, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: &writeWriteConflictError{
						Source:         cerr.Source,
						BucketName:     srcBucketName,
						ScopeName:      srcScopeName,
						CollectionName: srcCollectionName,
						DocumentKey:    srcDocID,
					},
					Class: TransactionErrorClassFailWriteWriteConflict,
				},
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	t.logger.Info("getting txn state")

	atrAgent, atrOboUser, err := t.bucketAgentProvider(atrBucketName)
	if err != nil {
		t.logger.Info("failed to get atr agent")

		return ecCb(nil, time.Time{}, classifyError(err))
	}

	err = t.hooks.BeforeCheckATREntryForBlockingDoc(ctx, []byte(atrDocID))
	if err != nil {
		return ecCb(nil, time.Time{}, classifyHookError(err))
	}

	result, err := atrAgent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		ScopeName:      atrScopeName,
		CollectionName: atrCollectionName,
		Key:            []byte(atrDocID),
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("attempts." + attemptID),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  memdx.SubdocXattrPathHLC,
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		OnBehalfOf: atrOboUser,
	})
	if err != nil {
		return ecCb(nil, time.Time{}, classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(nil, time.Time{}, classifyError(op.Err))
		}
	}

	var txnAttempt *AtrAttemptJson
	if err := json.Unmarshal(result.Ops[0].Value, &txnAttempt); err != nil {
		return ecCb(nil, time.Time{}, classifyError(err))
	}

	hlcNowTime, err := memdx.ParseHLCToTime(result.Ops[1].Value)
	if err != nil {
		return ecCb(nil, time.Time{}, classifyError(err))
	}

	pendingCas, err := memdx.ParseMacroCasToCas([]byte(txnAttempt.PendingCAS))
	if err != nil {
		return ecCb(nil, time.Time{}, classifyError(err))
	}

	hlcStartTime, err := memdx.ParseCasToTime(pendingCas)
	if err != nil {
		return ecCb(nil, time.Time{}, classifyError(err))
	}

	hlcExpiryTime := hlcStartTime.Add(time.Duration(txnAttempt.ExpiryTimeNanos))

	remainingExpiry := hlcExpiryTime.Sub(hlcNowTime)
	expiryTime := time.Now().Add(remainingExpiry)

	return ecCb(txnAttempt, expiryTime, nil)
}

func (t *TransactionAttempt) writeWriteConflictPoll(
	ctx context.Context,
	stage forwardCompatStage,
	bucketName string,
	scopeName string,
	collectionName string,
	key []byte,
	cas uint64,
	meta *MutableItemMeta,
	existingMutation *stagedMutation,
) *transactionOperationStatus {
	if meta == nil {
		t.logger.Info("meta is nil, no write-write conflict")

		// There is no write-write conflict.
		return nil
	}

	if meta.TransactionID == t.transactionID {
		if meta.AttemptID == t.id {
			if existingMutation != nil {
				if cas != existingMutation.Cas {
					// There was an existing mutation but it doesn't match the expected
					// CAS.  We throw a CAS mismatch to early detect this.
					return t.operationFailed(operationFailedDef{
						Cerr: &classifiedError{
							Source: errors.New("cas mismatch occured against local staged mutation"),
							Class:  TransactionErrorClassFailCasMismatch,
						},
						ShouldNotRetry:    false,
						ShouldNotRollback: false,
						Reason:            TransactionErrorReasonTransactionFailed,
					})
				}

				return nil
			}

			// This means that we are trying to overwrite a previous write this specific
			// attempt has performed without actually having found the existing mutation,
			// this is never going to work correctly.
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrIllegalState, "attempted to overwrite local staged mutation but couldn't find it")),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}

		t.logger.Info("transaction meta matches ours, no write-write conflict")

		// The transaction matches our transaction.  We can safely overwrite the existing
		// data in the txn meta and continue.
		return nil
	}

	retryCtx, retryCancel := context.WithTimeout(ctx, 1*time.Second)
	defer retryCancel()

	for {
		t.logger.Info("performing write-write conflict poll")

		ctxErr := retryCtx.Err()
		if ctxErr != nil {
			t.logger.Info("deadline expired during write-write poll")

			// If the deadline expired, lets just immediately return.
			return t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Source: &writeWriteConflictError{
						Source: fmt.Errorf(
							"deadline expired before WWC was resolved on %s.%s.%s.%s",
							meta.ATR.BucketName,
							meta.ATR.ScopeName,
							meta.ATR.CollectionName,
							meta.ATR.DocID),
						BucketName:     bucketName,
						ScopeName:      scopeName,
						CollectionName: collectionName,
						DocumentKey:    key,
					},
					Class: TransactionErrorClassFailWriteWriteConflict,
				},
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}

		err := t.checkForwardCompatability(ctx, key, bucketName, scopeName, collectionName, stage, meta.ForwardCompat, false)
		if err != nil {
			return err
		}

		cerr := t.checkExpiredAtomic(ctx, hookWWC, key, false)
		if cerr != nil {
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
		}

		attempt, _, err := t.getTxnState(
			ctx,
			bucketName,
			scopeName,
			collectionName,
			key,
			meta.ATR.BucketName,
			meta.ATR.ScopeName,
			meta.ATR.CollectionName,
			meta.ATR.DocID,
			meta.AttemptID,
			false)
		if err != nil {
			return err
		}

		if attempt == nil {
			t.logger.Info("atr entry missing, completing write-write conflict poll")
			return nil
		}

		state := TxnStateJson(attempt.State)
		if state == TxnStateJsonCompleted || state == TxnStateJsonRolledBack {
			t.logger.Info("attempt state finished, completing write-write conflict poll",
				zap.String("state", string(state)))
			// If we have progressed enough to continue, let's do that.
			return nil
		}

		select {
		case <-time.After(200 * time.Millisecond):
			// try again
		case <-ctx.Done():
			// loop back and the check at the top of the loop will
			// handle the error from the context for us.
		}
	}
}

func (t *TransactionAttempt) ensureCleanUpRequest() {
	t.lock.Lock()

	// when nothing is written, we can't do cleanup, and we expect not to have an atr agent
	// yet, however if we are past this state, we should be guarenteed to have one.
	if t.state == TransactionAttemptStateNothingWritten {
		t.lock.Unlock()
		t.logger.Info("attempt state nothing written, will not add cleanup request")
		return
	}

	if t.state == TransactionAttemptStateCompleted || t.state == TransactionAttemptStateRolledBack {
		t.lock.Unlock()
		t.logger.Info("attempt state completed or rolled back, will not add cleanup request")
		return
	}

	if t.hasCleanupRequest {
		t.lock.Unlock()
		t.logger.Info("attempt already created cleanup request, will not add cleanup request")
		return
	}

	t.hasCleanupRequest = true

	var inserts []TransactionCleanupDocRecord
	var replaces []TransactionCleanupDocRecord
	var removes []TransactionCleanupDocRecord
	for _, staged := range t.stagedMutations {
		dr := TransactionCleanupDocRecord{
			CollectionName: staged.CollectionName,
			ScopeName:      staged.ScopeName,
			Agent:          staged.Agent,
			OboUser:        staged.OboUser,
			ID:             staged.Key,
		}

		switch staged.OpType {
		case StagedMutationInsert:
			inserts = append(inserts, dr)
		case StagedMutationReplace:
			replaces = append(replaces, dr)
		case StagedMutationRemove:
			removes = append(removes, dr)
		}
	}

	cleanupState := t.state
	if cleanupState == TransactionAttemptStateCommitting {
		cleanupState = TransactionAttemptStatePending
	}

	req := &TransactionCleanupRequest{
		AttemptID:         t.id,
		AtrID:             t.atrKey,
		AtrCollectionName: t.atrCollectionName,
		AtrScopeName:      t.atrScopeName,
		AtrAgent:          t.atrAgent,
		AtrOboUser:        t.atrOboUser,
		Inserts:           inserts,
		Replaces:          replaces,
		Removes:           removes,
		State:             cleanupState,
		ForwardCompat:     nil, // Let's just be explicit about this, it'll change in the future anyway.
		DurabilityLevel:   t.durabilityLevel,
		TxnStartTime:      t.txnStartTime,
	}

	t.lock.Unlock()

	t.logger.Info("adding cleanup request",
		zaputils.FQDocID("atr", t.atrAgent.BucketName(), t.atrScopeName, t.atrCollectionName, t.atrKey),
		zap.Stringer("state", cleanupState))

	t.cleanupQueue.AddRequest(req)
}

func (t *TransactionAttempt) result() *TransactionAttemptResult {
	t.lock.Lock()
	stateBits := atomic.LoadUint32(&t.stateBits)

	if t.state == TransactionAttemptStateNothingWritten {
		t.lock.Unlock()
		return &TransactionAttemptResult{
			State:                 t.state,
			ID:                    t.id,
			AtrBucketName:         "",
			AtrScopeName:          "",
			AtrCollectionName:     "",
			AtrID:                 []byte(""),
			UnstagingComplete:     true,
			Expired:               false,
			PreExpiryAutoRollback: false,
		}
	}

	result := &TransactionAttemptResult{
		State:                 t.state,
		ID:                    t.id,
		AtrBucketName:         t.atrAgent.BucketName(),
		AtrScopeName:          t.atrScopeName,
		AtrCollectionName:     t.atrCollectionName,
		AtrID:                 t.atrKey,
		UnstagingComplete:     t.state == TransactionAttemptStateCompleted || t.state == TransactionAttemptStateRolledBack,
		Expired:               (stateBits & transactionStateBitHasExpired) != 0,
		PreExpiryAutoRollback: (stateBits & transactionStateBitPreExpiryAutoRollback) != 0,
	}
	t.lock.Unlock()

	return result
}
