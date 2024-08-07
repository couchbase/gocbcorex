package transactionsx

import (
	"context"
	"encoding/json"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

func (t *TransactionAttempt) Get(ctx context.Context, opts GetOptions) (*GetResult, error) {
	result, errSt := t.get(ctx, opts)
	if errSt != nil {
		t.logger.Info("get failed", zap.Error(errSt.Err()))
		return nil, errSt.Err()
	}

	return result, nil
}

func (t *TransactionAttempt) get(
	ctx context.Context,
	opts GetOptions,
) (*GetResult, *transactionOperationStatus) {
	forceNonFatal := t.enableNonFatalGets

	t.logger.Info("performing get",
		zaputils.FQDocID("doc", opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName, opts.Key),
		zap.Bool("forceNonFatal", forceNonFatal))

	t.lock.Lock()
	t.beginOpLocked()

	oerr := t.checkCanPerformOpLocked()
	if oerr != nil {
		t.lock.Unlock()
		t.endOp()
		return nil, oerr
	}

	t.lock.Unlock()

	expired := t.checkExpiredAtomic(ctx, hookStageGet, opts.Key, false)
	if expired {
		t.endOp()
		return nil, t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Source: ErrAttemptExpired,
				Class:  TransactionErrorClassFailExpiry,
			},
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	result, oErr := t.mavRead(ctx, opts.Agent, opts.OboUser, opts.ScopeName, opts.CollectionName, opts.Key, opts.NoRYOW,
		"", forceNonFatal)
	if oErr != nil {
		t.endOp()
		return nil, oErr
	}

	if t.hooks.AfterGetComplete != nil {
		err := t.hooks.AfterGetComplete(ctx, opts.Key)
		if err != nil {
			t.endOp()
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              classifyHookError(err),
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	t.endOp()
	return result, nil
}

func (t *TransactionAttempt) mavRead(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	disableRYOW bool,
	resolvingATREntry string,
	forceNonFatal bool,
) (*GetResult, *transactionOperationStatus) {
	doc, err := t.fetchDocWithMeta(
		ctx,
		agent,
		oboUser,
		scopeName,
		collectionName,
		key,
		forceNonFatal)
	if err != nil {
		return nil, err
	}

	if disableRYOW {
		if doc.TxnMeta != nil && doc.TxnMeta.ID.Attempt == t.id {
			t.logger.Info("ryow disabled and tnx meta is not nil, resetting meta to nil")

			// This is going to be a RYOW, we can just clear the TxnMeta which
			// will cause us to fall into the block below.
			doc.TxnMeta = nil
		}
	}

	// Doc not involved in another transaction.
	if doc.TxnMeta == nil {
		if doc.Deleted {
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocNotFound, "txn meta is nil, doc was a tombstone"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		}

		t.logger.Info("txn meta is nil, returning result")

		return &GetResult{
			agent:          agent,
			oboUser:        oboUser,
			scopeName:      scopeName,
			collectionName: collectionName,
			key:            key,
			Value:          doc.Body,
			Cas:            doc.Cas,
			Meta:           nil,
		}, nil
	}

	if doc.TxnMeta.ID.Attempt == t.id {
		switch doc.TxnMeta.Operation.Type {
		case MutationTypeJsonInsert:
			t.logger.Info("doc already in txn as insert, using staged value")

			return &GetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
			}, nil
		case MutationTypeJsonReplace:
			t.logger.Info("doc already in txn as replace, using staged value")

			return &GetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
			}, nil
		case MutationTypeJsonRemove:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocNotFound, "doc was a staged remove"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		default:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrIllegalState, "unexpected staged mutation type")),
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	if doc.TxnMeta.ID.Attempt == resolvingATREntry {
		if doc.Deleted {
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocNotFound, "doc was a staged tombstone during resolution"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		}

		t.logger.Info("completed ATR resolution")

		return &GetResult{
			agent:          agent,
			oboUser:        oboUser,
			scopeName:      scopeName,
			collectionName: collectionName,
			key:            key,
			Value:          doc.Body,
			Cas:            doc.Cas,
		}, nil
	}

	docFc := forwardCompatFromJson(doc.TxnMeta.ForwardCompat)
	docMeta := &MutableItemMeta{
		TransactionID: doc.TxnMeta.ID.Transaction,
		AttemptID:     doc.TxnMeta.ID.Attempt,
		ATR: MutableItemMetaATR{
			BucketName:     doc.TxnMeta.ATR.BucketName,
			ScopeName:      doc.TxnMeta.ATR.ScopeName,
			CollectionName: doc.TxnMeta.ATR.CollectionName,
			DocID:          doc.TxnMeta.ATR.DocID,
		},
		ForwardCompat: docFc,
	}

	oerr := t.checkForwardCompatability(
		ctx,
		key,
		agent.BucketName(),
		scopeName,
		collectionName,
		forwardCompatStageGets,
		docFc,
		forceNonFatal)
	if oerr != nil {
		return nil, oerr
	}

	attempt, oerr := t.getTxnState(
		ctx,
		agent.BucketName(),
		scopeName,
		collectionName,
		key,
		doc.TxnMeta.ATR.BucketName,
		doc.TxnMeta.ATR.ScopeName,
		doc.TxnMeta.ATR.CollectionName,
		doc.TxnMeta.ATR.DocID,
		doc.TxnMeta.ID.Attempt,
		forceNonFatal)
	if oerr != nil {
		return nil, oerr
	}

	if attempt == nil {
		t.logger.Info("atr entry missing, rerunning mav read")

		// The ATR entry is missing, it's likely that we just raced the other transaction
		// cleaning up it's documents and then cleaning itself up.  Lets run ATR resolution.
		return t.mavRead(ctx, agent, oboUser, scopeName, collectionName, key, disableRYOW, doc.TxnMeta.ID.Attempt, forceNonFatal)
	}

	atmptFc := forwardCompatFromJson(attempt.entry.ForwardCompat)
	oerr = t.checkForwardCompatability(
		ctx,
		key,
		agent.BucketName(),
		scopeName,
		collectionName,
		forwardCompatStageGetsReadingATR, atmptFc, forceNonFatal)
	if oerr != nil {
		return nil, oerr
	}

	state := TxnStateJson(attempt.entry.State)
	if state == TxnStateJsonCommitted || state == TxnStateJsonCompleted {
		switch doc.TxnMeta.Operation.Type {
		case MutationTypeJsonInsert:
			t.logger.Info("doc already in txn as insert, using staged value")

			return &GetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
				Meta:           docMeta,
			}, nil
		case MutationTypeJsonReplace:
			t.logger.Info("doc already in txn as replace, using staged value")

			return &GetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
				Meta:           docMeta,
			}, nil
		case MutationTypeJsonRemove:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocNotFound, "doc was a staged remove"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		default:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: classifyError(
					wrapError(ErrIllegalState, "unexpected staged mutation type")),
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
			})
		}
	}

	if doc.Deleted {
		return nil, t.operationFailed(operationFailedDef{
			Cerr: &classifiedError{
				Class:  TransactionErrorClassFailOther,
				Source: wrapError(ErrDocNotFound, "doc was a tombstone"),
			},
			CanStillCommit:    true,
			ShouldNotRetry:    false,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonSuccess,
		})
	}

	return &GetResult{
		agent:          agent,
		oboUser:        oboUser,
		scopeName:      scopeName,
		collectionName: collectionName,
		key:            key,
		Value:          doc.Body,
		Cas:            doc.Cas,
		Meta:           docMeta,
	}, nil
}

type docWithMeta struct {
	Body    []byte
	TxnMeta *TxnXattrJson
	DocMeta *TxnXattrDocMetaJson
	Cas     uint64
	Deleted bool
}

func (t *TransactionAttempt) fetchDocWithMeta(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	forceNonFatal bool,
) (*docWithMeta, *transactionOperationStatus) {
	res, err := invokeHookWithDocID(ctx, t.hooks.DocGet, key, func() (*docWithMeta, error) {
		result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Key:            key,
			Ops: []memdx.LookupInOp{
				{
					Op:    memdx.LookupInOpTypeGet,
					Path:  []byte("$document"),
					Flags: memdx.SubdocOpFlagXattrPath,
				},
				{
					Op:    memdx.LookupInOpTypeGet,
					Path:  []byte("txn"),
					Flags: memdx.SubdocOpFlagXattrPath,
				},
				{
					Op:    memdx.LookupInOpTypeGetDoc,
					Path:  nil,
					Flags: memdx.SubdocOpFlagNone,
				},
			},
			Flags:      memdx.SubdocDocFlagAccessDeleted,
			OnBehalfOf: oboUser,
		})
		if err != nil {
			return nil, err
		}

		if result.Ops[0].Err != nil {
			return nil, result.Ops[0].Err
		}

		var meta *TxnXattrDocMetaJson
		if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
			return nil, err
		}

		var txnMeta *TxnXattrJson
		if result.Ops[1].Err == nil {
			// Doc is currently in a txn.
			var txnMetaVal TxnXattrJson
			if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
				return nil, err
			}

			txnMeta = &txnMetaVal
		}

		var docBody []byte
		if result.Ops[2].Err == nil {
			docBody = result.Ops[2].Value
		}

		return &docWithMeta{
			Body:    docBody,
			TxnMeta: txnMeta,
			DocMeta: meta,
			Cas:     result.Cas,
			Deleted: result.DocIsDeleted,
		}, nil
	})
	if err != nil {
		cerr := classifyError(err)
		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			return nil, t.operationFailed(operationFailedDef{
				Cerr: &classifiedError{
					Class:  TransactionErrorClassFailOther,
					Source: wrapError(ErrDocNotFound, "doc was not found"),
				},
				CanStillCommit:    true,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonSuccess,
			})
		case TransactionErrorClassFailTransient:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    false,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		case TransactionErrorClassFailHard:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: true,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		default:
			return nil, t.operationFailed(operationFailedDef{
				Cerr:              cerr,
				CanStillCommit:    forceNonFatal,
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            TransactionErrorReasonTransactionFailed,
			})
		}
	}

	return res, nil
}
