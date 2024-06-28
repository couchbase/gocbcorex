package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
)

func (t *TransactionAttempt) selectAtrKey(
	ctx context.Context,
	firstKey []byte,
) ([]byte, *transactionOperationStatus) {
	hookAtrID, err := t.hooks.RandomATRIDForVbucket(ctx)
	if err != nil {
		return nil, t.operationFailed(operationFailedDef{
			Cerr:              classifyHookError(err),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}

	if hookAtrID != "" {
		return []byte(hookAtrID), nil
	}

	crc := crc32.ChecksumIEEE(firstKey)
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	atrIdx := int(crcMidBits % uint16(t.numAtrs))

	atrKey := []byte(AtrIDList[atrIdx])

	return atrKey, nil
}

func (t *TransactionAttempt) selectAtrExclusive(
	ctx context.Context,
	firstAgent *gocbcorex.Agent,
	firstOboUser string,
	firstKey []byte,
) *transactionOperationStatus {
	atrKey, err := t.selectAtrKey(ctx, firstKey)
	if err != nil {
		return err
	}

	atrAgent := firstAgent
	atrOboUser := firstOboUser
	atrScopeName := "_default"
	atrCollectionName := "_default"
	if t.atrLocation.Agent != nil {
		atrAgent = t.atrLocation.Agent
		atrOboUser = t.atrLocation.OboUser
		atrScopeName = t.atrLocation.ScopeName
		atrCollectionName = t.atrLocation.CollectionName
	} else {
		if t.enableExplicitATRs {
			return t.operationFailed(operationFailedDef{
				Cerr:              classifyError(errors.New("atrs must be explicitly defined")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	t.lock.Lock()

	t.atrAgent = atrAgent
	t.atrOboUser = atrOboUser
	t.atrScopeName = atrScopeName
	t.atrCollectionName = atrCollectionName
	t.atrKey = atrKey

	t.lock.Unlock()

	return nil
}

func (t *TransactionAttempt) setATRPendingExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
		if cerr == nil {
			return nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.setATRPendingExclusive(ctx)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailPathAlreadyExists:
			return nil
		case TransactionErrorClassFailExpiry:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionExpired,
			})
		case TransactionErrorClassFailOutOfSpace:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailTransient:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookATRPending, nil, false)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeATRPending(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	var marshalErr error
	atrFieldOp := func(fieldName string, data interface{}, flags memdx.SubdocOpFlag) memdx.MutateInOp {
		b, err := json.Marshal(data)
		if err != nil {
			marshalErr = err
			return memdx.MutateInOp{}
		}

		return memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictAdd,
			Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagMkDirP | flags,
			Path:  []byte("attempts." + t.id + "." + fieldName),
			Value: b,
		}
	}

	atrOps := []memdx.MutateInOp{
		atrFieldOp("tst", "${Mutation.CAS}", memdx.SubdocOpFlagExpandMacros),
		atrFieldOp("tid", t.transactionID, memdx.SubdocOpFlagNone),
		atrFieldOp("st", TxnStateJsonPending, memdx.SubdocOpFlagNone),
		atrFieldOp("exp", time.Until(t.expiryTime)/time.Millisecond, memdx.SubdocOpFlagNone),
		atrFieldOp("d", durabilityLevelToJson(t.durabilityLevel), memdx.SubdocOpFlagNone),
		{
			Op:    memdx.MutateInOpTypeSetDoc,
			Flags: memdx.SubdocOpFlagNone,
			Path:  nil,
			Value: []byte{0},
		},
	}
	if marshalErr != nil {
		return ecCb(classifyError(marshalErr))
	}

	t.logger.Info("setting atr pending",
		zaputils.FQDocID("atr", t.atrAgent.BucketName(), t.atrScopeName, t.atrCollectionName, t.atrKey))

	memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
	if err != nil {
		return ecCb(classifyError(err))
	}

	result, err := t.atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:       t.atrScopeName,
		CollectionName:  t.atrCollectionName,
		Key:             t.atrKey,
		Ops:             atrOps,
		Flags:           memdx.SubdocDocFlagMkDoc,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      t.atrOboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterATRPending(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	if t.lostCleanupSystem != nil {
		t.lostCleanupSystem.AddLocation(LostCleanupLocation{
			Agent:          t.atrAgent,
			OboUser:        t.atrOboUser,
			ScopeName:      t.atrScopeName,
			CollectionName: t.atrCollectionName,
			NumATRs:        1024,
		})
	}

	return nil
}

func (t *TransactionAttempt) fetchATRCommitConflictExclusive(
	ctx context.Context,
) (TxnStateJson, *transactionOperationStatus) {
	ecCb := func(st TxnStateJson, cerr *classifiedError) (TxnStateJson, *transactionOperationStatus) {
		if cerr == nil {
			return st, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailTransient:
			fallthrough
		case TransactionErrorClassFailOther:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.fetchATRCommitConflictExclusive(ctx)
			case <-ctx.Done():
				return TxnStateJsonUnknown, t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocNotFound:
			return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
			})
		case TransactionErrorClassFailPathNotFound:
			return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
			})
		case TransactionErrorClassFailExpiry:
			return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
			})
		case TransactionErrorClassFailHard:
			return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
			})
		default:
			return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
			})
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookATRCommitAmbiguityResolution, nil, false)
	if cerr != nil {
		return ecCb(TxnStateJsonUnknown, cerr)
	}

	err := t.hooks.BeforeATRCommitAmbiguityResolution(ctx)
	if err != nil {
		return ecCb(TxnStateJsonUnknown, classifyHookError(err))
	}

	result, err := t.atrAgent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		ScopeName:      t.atrScopeName,
		CollectionName: t.atrCollectionName,
		Key:            t.atrKey,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("attempts." + t.id + ".st"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags:      memdx.SubdocDocFlagNone,
		OnBehalfOf: t.atrOboUser,
	})
	if err != nil {
		return ecCb(TxnStateJsonUnknown, classifyError(err))
	}

	if result.Ops[0].Err != nil {
		return ecCb(TxnStateJsonUnknown, classifyError(err))
	}

	var st TxnStateJson
	if err := json.Unmarshal(result.Ops[0].Value, &st); err != nil {
		return ecCb(TxnStateJsonUnknown, classifyError(err))
	}

	return st, nil
}

func (t *TransactionAttempt) resolveATRCommitConflictExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	st, err := t.fetchATRCommitConflictExclusive(ctx)
	if err != nil {
		return err
	}

	switch st {
	case TxnStateJsonPending:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction still pending even with p set during commit")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TxnStateJsonCommitted:
		return nil
	case TxnStateJsonCompleted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already completed during commit")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TxnStateJsonAborted:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already aborted during commit")),
			ShouldNotRetry:    false,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	case TxnStateJsonRolledBack:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, "transaction already rolled back during commit")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	default:
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrIllegalState, fmt.Sprintf("illegal transaction state during commit: %s", st))),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailed,
		})
	}
}

func (t *TransactionAttempt) setATRCommittedExclusive(
	ctx context.Context,
	ambiguityResolution bool,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
		if cerr == nil {
			return nil
		}

		errorReason := TransactionErrorReasonTransactionFailed
		if ambiguityResolution {
			errorReason = TransactionErrorReasonTransactionCommitAmbiguous
		}

		switch cerr.Class {
		case TransactionErrorClassFailAmbiguous:
			select {
			case <-time.After(3 * time.Millisecond):
				ambiguityResolution = true
				return t.setATRCommittedExclusive(ctx, ambiguityResolution)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailTransient:
			if ambiguityResolution {
				select {
				case <-time.After(3 * time.Millisecond):
					return t.setATRCommittedExclusive(ctx, ambiguityResolution)
				case <-ctx.Done():
					return t.contextFailed(ctx.Err())
				}
			}

			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            errorReason,
			})
		case TransactionErrorClassFailPathAlreadyExists:
			return t.resolveATRCommitConflictExclusive(ctx)
		case TransactionErrorClassFailDocNotFound:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			})
		case TransactionErrorClassFailPathNotFound:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			})
		case TransactionErrorClassFailOutOfSpace:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			})
		case TransactionErrorClassFailExpiry:
			if errorReason == TransactionErrorReasonTransactionFailed {
				errorReason = TransactionErrorReasonTransactionExpired
			}

			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            errorReason,
			})
		default:
			if ambiguityResolution {
				return t.operationFailed(operationFailedDef{
					Cerr:              cerr,
					ShouldNotRetry:    true,
					ShouldNotRollback: true,
					Reason:            errorReason,
				})
			}

			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            errorReason,
			})
		}
	}

	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	insMutations := []AtrMutationJson{}
	repMutations := []AtrMutationJson{}
	remMutations := []AtrMutationJson{}

	for _, mutation := range t.stagedMutations {
		jsonMutation := AtrMutationJson{
			BucketName:     mutation.Agent.BucketName(),
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			DocID:          string(mutation.Key),
		}

		if mutation.OpType == StagedMutationInsert {
			insMutations = append(insMutations, jsonMutation)
		} else if mutation.OpType == StagedMutationReplace {
			repMutations = append(repMutations, jsonMutation)
		} else if mutation.OpType == StagedMutationRemove {
			remMutations = append(remMutations, jsonMutation)
		} else {
			return ecCb(classifyError(wrapError(ErrIllegalState, "unexpected staged mutation type")))
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookATRCommit, nil, false)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeATRCommit(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	var marshalErr error
	atrFieldOp := func(fieldName string, data interface{}, flags memdx.SubdocOpFlag, op memdx.MutateInOpType) memdx.MutateInOp {
		bytes, err := json.Marshal(data)
		if err != nil {
			marshalErr = err
		}

		return memdx.MutateInOp{
			Op:    op,
			Flags: memdx.SubdocOpFlagXattrPath | flags,
			Path:  []byte("attempts." + t.id + "." + fieldName),
			Value: bytes,
		}
	}

	atrOps := []memdx.MutateInOp{
		atrFieldOp("st", TxnStateJsonCommitted, memdx.SubdocOpFlagNone, memdx.MutateInOpTypeDictSet),
		atrFieldOp("tsc", "${Mutation.CAS}", memdx.SubdocOpFlagExpandMacros, memdx.MutateInOpTypeDictSet),
		atrFieldOp("p", 0, memdx.SubdocOpFlagNone, memdx.MutateInOpTypeDictAdd),
		atrFieldOp("ins", insMutations, memdx.SubdocOpFlagNone, memdx.MutateInOpTypeDictSet),
		atrFieldOp("rep", repMutations, memdx.SubdocOpFlagNone, memdx.MutateInOpTypeDictSet),
		atrFieldOp("rem", remMutations, memdx.SubdocOpFlagNone, memdx.MutateInOpTypeDictSet),
	}
	if marshalErr != nil {
		return ecCb(classifyError(marshalErr))
	}

	memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
	if err != nil {
		return ecCb(classifyError(err))
	}

	result, err := atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:       atrScopeName,
		CollectionName:  atrCollectionName,
		Key:             atrKey,
		Ops:             atrOps,
		Flags:           memdx.SubdocDocFlagNone,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      atrOboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterATRCommit(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *TransactionAttempt) setATRCompletedExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "completed atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailPathNotFound:
			// This is technically a full success, but FIT expects unstagingCompleted=false...
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		case TransactionErrorClassFailExpiry:
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "completed atr removal operation expired")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		default:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}
	}

	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	cerr := t.checkExpiredAtomic(ctx, hookATRComplete, nil, true)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeATRComplete(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	atrOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte("attempts." + t.id),
		},
	}

	memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
	if err != nil {
		return ecCb(classifyError(err))
	}

	result, err := atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:       atrScopeName,
		CollectionName:  atrCollectionName,
		Key:             atrKey,
		Ops:             atrOps,
		Flags:           memdx.SubdocDocFlagNone,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      atrOboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterATRComplete(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *TransactionAttempt) setATRAbortedExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "atr abort failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailExpiry:
			t.setExpiryOvertimeAtomic()

			select {
			case <-time.After(3 * time.Millisecond):
				return t.setATRAbortedExclusive(ctx)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		case TransactionErrorClassFailDocNotFound:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailPathNotFound:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrEntryNotFound),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailOutOfSpace:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr.Wrap(ErrAtrFull),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		default:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.setATRAbortedExclusive(ctx)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		}
	}

	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	insMutations := []AtrMutationJson{}
	repMutations := []AtrMutationJson{}
	remMutations := []AtrMutationJson{}

	for _, mutation := range t.stagedMutations {
		jsonMutation := AtrMutationJson{
			BucketName:     mutation.Agent.BucketName(),
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			DocID:          string(mutation.Key),
		}

		if mutation.OpType == StagedMutationInsert {
			insMutations = append(insMutations, jsonMutation)
		} else if mutation.OpType == StagedMutationReplace {
			repMutations = append(repMutations, jsonMutation)
		} else if mutation.OpType == StagedMutationRemove {
			remMutations = append(remMutations, jsonMutation)
		} else {
			return ecCb(classifyError(wrapError(ErrIllegalState, "unexpected staged mutation type")))
		}
	}

	cerr := t.checkExpiredAtomic(ctx, hookATRAbort, nil, true)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeATRAborted(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	var marshalErr error
	atrFieldOp := func(fieldName string, data interface{}, flags memdx.SubdocOpFlag) memdx.MutateInOp {
		bytes, err := json.Marshal(data)
		if err != nil {
			marshalErr = err
		}

		return memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Flags: memdx.SubdocOpFlagXattrPath | flags,
			Path:  []byte("attempts." + t.id + "." + fieldName),
			Value: bytes,
		}
	}

	atrOps := []memdx.MutateInOp{
		atrFieldOp("st", TxnStateJsonAborted, memdx.SubdocOpFlagNone),
		atrFieldOp("tsrs", "${Mutation.CAS}", memdx.SubdocOpFlagExpandMacros),
		atrFieldOp("ins", insMutations, memdx.SubdocOpFlagNone),
		atrFieldOp("rep", repMutations, memdx.SubdocOpFlagNone),
		atrFieldOp("rem", remMutations, memdx.SubdocOpFlagNone),
	}
	if marshalErr != nil {
		return ecCb(classifyError(marshalErr))
	}

	memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
	if err != nil {
		return ecCb(classifyError(err))
	}

	result, err := atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:       atrScopeName,
		CollectionName:  atrCollectionName,
		Key:             atrKey,
		Ops:             atrOps,
		Flags:           memdx.SubdocDocFlagNone,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      atrOboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterATRAborted(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil
}

func (t *TransactionAttempt) setATRRolledBackExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	ecCb := func(cerr *classifiedError) *transactionOperationStatus {
		if cerr == nil {
			return nil
		}

		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "rolled back atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailPathNotFound:
			return nil
		case TransactionErrorClassFailExpiry:
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "rolled back atr removal operation expired")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		case TransactionErrorClassFailHard:
			return t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		default:
			select {
			case <-time.After(3 * time.Millisecond):
				return t.setATRRolledBackExclusive(ctx)
			case <-ctx.Done():
				return t.contextFailed(ctx.Err())
			}
		}
	}

	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	cerr := t.checkExpiredAtomic(ctx, hookATRRollback, nil, true)
	if cerr != nil {
		return ecCb(cerr)
	}

	err := t.hooks.BeforeATRRolledBack(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	atrOps := []memdx.MutateInOp{
		{
			Op:    memdx.MutateInOpTypeDelete,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte("attempts." + t.id),
		},
	}

	memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
	if err != nil {
		return ecCb(classifyError(err))
	}

	result, err := atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		ScopeName:       atrScopeName,
		CollectionName:  atrCollectionName,
		Key:             atrKey,
		Ops:             atrOps,
		Flags:           memdx.SubdocDocFlagNone,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      atrOboUser,
	})
	if err != nil {
		return ecCb(classifyError(err))
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			return ecCb(classifyError(op.Err))
		}
	}

	err = t.hooks.AfterATRRolledBack(ctx)
	if err != nil {
		return ecCb(classifyHookError(err))
	}

	return nil

}
