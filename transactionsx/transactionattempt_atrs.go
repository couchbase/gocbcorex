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
	if t.hooks.RandomATRIDForVbucket != nil {
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
	expired := t.checkExpiredAtomic(ctx, hookStageATRPending, nil, false)
	if expired {
		return t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	err := invokeNoResHook(ctx, t.hooks.ATRPending, func() error {
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
			return marshalErr
		}

		t.logger.Info("setting atr pending",
			zaputils.FQDocID("atr", t.atrAgent.BucketName(), t.atrScopeName, t.atrCollectionName, t.atrKey))

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		cerr := classifyError(err)
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
	expired := t.checkExpiredAtomic(ctx, hookStageATRCommitAmbiguityResolution, nil, false)
	if expired {
		return TxnStateJsonUnknown, t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionCommitAmbiguous,
		})
	}

	txnState, err := invokeHook(ctx, t.hooks.ATRCommitAmbiguityResolution, func() (TxnStateJson, error) {
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
			return TxnStateJsonUnknown, err
		}

		if result.Ops[0].Err != nil {
			return TxnStateJsonUnknown, err
		}

		var st TxnStateJson
		if err := json.Unmarshal(result.Ops[0].Value, &st); err != nil {
			return TxnStateJsonUnknown, err
		}

		return st, nil
	})
	if err != nil {
		cerr := classifyError(err)
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

	return txnState, nil
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
	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	expired := t.checkExpiredAtomic(ctx, hookStageATRCommit, nil, false)
	if expired {
		errorReason := TransactionErrorReasonTransactionExpired
		if ambiguityResolution {
			errorReason = TransactionErrorReasonTransactionCommitAmbiguous
		}

		return t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            errorReason,
		})
	}

	err := invokeNoResHook(ctx, t.hooks.ATRCommit, func() error {
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

			switch mutation.OpType {
			case StagedMutationInsert:
				insMutations = append(insMutations, jsonMutation)
			case StagedMutationReplace:
				repMutations = append(repMutations, jsonMutation)
			case StagedMutationRemove:
				remMutations = append(remMutations, jsonMutation)
			default:
				return wrapError(ErrIllegalState, "unexpected staged mutation type")
			}
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
			return marshalErr
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		cerr := classifyError(err)
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

	return nil
}

func (t *TransactionAttempt) setATRCompletedExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	expired := t.checkExpiredAtomic(ctx, hookStageATRComplete, nil, true)
	if expired {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrAttemptExpired, "transaction expired during completed atr removal")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            TransactionErrorReasonTransactionFailedPostCommit,
		})
	}

	err := invokeNoResHook(ctx, t.hooks.ATRComplete, func() error {
		atrOps := []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDelete,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("attempts." + t.id),
			},
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "completed atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailedPostCommit,
			})
		}

		cerr := classifyError(err)
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

	return nil
}

func (t *TransactionAttempt) setATRAbortedExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	expired := t.checkExpiredAtomic(ctx, hookStageATRAbort, nil, true)
	if expired {
		t.setExpiryOvertimeAtomic()
	}

	err := invokeNoResHook(ctx, t.hooks.ATRAborted, func() error {
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

			switch mutation.OpType {
			case StagedMutationInsert:
				insMutations = append(insMutations, jsonMutation)
			case StagedMutationReplace:
				repMutations = append(repMutations, jsonMutation)
			case StagedMutationRemove:
				remMutations = append(remMutations, jsonMutation)
			default:
				return wrapError(ErrIllegalState, "unexpected staged mutation type")
			}
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
			return marshalErr
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "atr abort failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		cerr := classifyError(err)
		switch cerr.Class {
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

	return nil
}

func (t *TransactionAttempt) setATRRolledBackExclusive(
	ctx context.Context,
) *transactionOperationStatus {
	atrAgent := t.atrAgent
	atrOboUser := t.atrOboUser
	atrScopeName := t.atrScopeName
	atrKey := t.atrKey
	atrCollectionName := t.atrCollectionName

	expired := t.checkExpiredAtomic(ctx, hookStageATRRollback, nil, true)
	if expired {
		return t.operationFailed(operationFailedDef{
			Cerr: classifyError(
				wrapError(ErrAttemptExpired, "transaction expired before rolled back atr removal")),
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
		})
	}

	err := invokeNoResHook(ctx, t.hooks.ATRRolledBack, func() error {
		atrOps := []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDelete,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("attempts." + t.id),
			},
		}

		memdDuraLevel, err := durabilityLevelToMemdx(t.durabilityLevel)
		if err != nil {
			return err
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
			return err
		}

		for _, op := range result.Ops {
			if op.Err != nil {
				return op.Err
			}
		}

		return nil
	})
	if err != nil {
		if t.isExpiryOvertimeAtomic() {
			return t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrAttemptExpired, "rolled back atr removal failed during overtime")),
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
			})
		}

		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			fallthrough
		case TransactionErrorClassFailPathNotFound:
			return nil
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

	return nil
}
