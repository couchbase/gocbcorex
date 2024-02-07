package transactionsx

import (
	"context"
	"encoding/json"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

func (t *transactionAttempt) Get(ctx context.Context, opts TransactionGetOptions) (*TransactionGetResult, error) {
	result, err := t.get(ctx, opts)
	if err != nil {
		t.logger.Info("Get failed", zap.Error(err))

		if !t.ShouldRollback() {
			t.ensureCleanUpRequest()
		}

		return nil, err
	}

	return result, nil
}

func (t *transactionAttempt) get(
	ctx context.Context,
	opts TransactionGetOptions,
) (*TransactionGetResult, error) {
	forceNonFatal := t.enableNonFatalGets

	t.logger.Info("Performing get",
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

	cerr := t.checkExpiredAtomic(ctx, hookGet, opts.Key, false)
	if cerr != nil {
		t.endOp()
		return nil, t.operationFailed(operationFailedDef{
			Cerr:              cerr,
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            TransactionErrorReasonTransactionExpired,
		})
	}

	result, err := t.mavRead(ctx, opts.Agent, opts.OboUser, opts.ScopeName, opts.CollectionName, opts.Key, opts.NoRYOW,
		"", forceNonFatal)
	if err != nil {
		t.endOp()
		return nil, err
	}

	err = t.hooks.AfterGetComplete(ctx, opts.Key)
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

	t.endOp()
	return result, nil
}

func (t *transactionAttempt) mavRead(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	disableRYOW bool,
	resolvingATREntry string,
	forceNonFatal bool,
) (*TransactionGetResult, error) {
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
			t.logger.Info("RYOW disabled and tnx meta is not nil, resetting meta to nil")

			// This is going to be a RYOW, we can just clear the TxnMeta which
			// will cause us to fall into the block below.
			doc.TxnMeta = nil
		}
	}

	// Doc not involved in another transaction.
	if doc.TxnMeta == nil {
		if doc.Deleted {
			// TODO(brett19): Check that this is the right error handling...
			return nil, wrapError(memdx.ErrDocNotFound, "doc was a tombstone")
		}

		t.logger.Info("Txn meta is nil, returning result")

		return &TransactionGetResult{
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
		case jsonMutationInsert:
			t.logger.Info("Doc already in txn as insert, using staged value")

			return &TransactionGetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
			}, nil
		case jsonMutationReplace:
			t.logger.Info("Doc already in txn as replace, using staged value")

			return &TransactionGetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
			}, nil
		case jsonMutationRemove:
			// TODO(brett19): Check that this is the right error handling...
			return nil, wrapError(memdx.ErrDocNotFound, "doc was a staged remove")
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
			// TODO(brett19): Consider if this is the right error...
			return nil, wrapError(memdx.ErrDocNotFound, "doc was a staged tombstone during resolution")
		}

		t.logger.Info("Completed ATR resolution")

		return &TransactionGetResult{
			agent:          agent,
			oboUser:        oboUser,
			scopeName:      scopeName,
			collectionName: collectionName,
			key:            key,
			Value:          doc.Body,
			Cas:            doc.Cas,
		}, nil
	}

	docFc := jsonForwardCompatToForwardCompat(doc.TxnMeta.ForwardCompat)
	docMeta := &TransactionMutableItemMeta{
		TransactionID: doc.TxnMeta.ID.Transaction,
		AttemptID:     doc.TxnMeta.ID.Attempt,
		ATR: TransactionMutableItemMetaATR{
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

	attempt, _, oerr := t.getTxnState(
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
		t.logger.Info("ATR entry missing, rerunning mav read")

		// The ATR entry is missing, it's likely that we just raced the other transaction
		// cleaning up it's documents and then cleaning itself up.  Lets run ATR resolution.
		return t.mavRead(ctx, agent, oboUser, scopeName, collectionName, key, disableRYOW, doc.TxnMeta.ID.Attempt, forceNonFatal)
	}

	atmptFc := jsonForwardCompatToForwardCompat(attempt.ForwardCompat)
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

	state := jsonAtrState(attempt.State)
	if state == jsonAtrStateCommitted || state == jsonAtrStateCompleted {
		switch doc.TxnMeta.Operation.Type {
		case jsonMutationInsert:
			t.logger.Info("Doc already in txn as insert, using staged value")

			return &TransactionGetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
				Meta:           docMeta,
			}, nil
		case jsonMutationReplace:
			t.logger.Info("Doc already in txn as replace, using staged value")

			return &TransactionGetResult{
				agent:          agent,
				oboUser:        oboUser,
				scopeName:      scopeName,
				collectionName: collectionName,
				key:            key,
				Value:          doc.TxnMeta.Operation.Staged,
				Cas:            doc.Cas,
				Meta:           docMeta,
			}, nil
		case jsonMutationRemove:
			// TODO(brett19): Consider if this is the right error
			return nil, wrapError(memdx.ErrDocNotFound, "doc was a staged remove")
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
		// TODO(brett19): More doc not found stuff...
		return nil, wrapError(memdx.ErrDocNotFound, "doc was a tombstone")
	}

	return &TransactionGetResult{
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

func (t *transactionAttempt) fetchDocWithMeta(
	ctx context.Context,
	agent *gocbcorex.Agent,
	oboUser string,
	scopeName string,
	collectionName string,
	key []byte,
	forceNonFatal bool,
) (*transactionGetDoc, error) {
	ecCb := func(doc *transactionGetDoc, cerr *classifiedError) (*transactionGetDoc, error) {
		if cerr == nil {
			return doc, nil
		}

		switch cerr.Class {
		case TransactionErrorClassFailDocNotFound:
			// TODO(brett19): More doc not found
			return nil, wrapError(memdx.ErrDocNotFound, "doc was not found")
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

	err := t.hooks.BeforeDocGet(ctx, key)
	if err != nil {
		return ecCb(nil, classifyHookError(err))
	}

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
		return ecCb(nil, classifyError(err))
	}

	if result.Ops[0].Err != nil {
		return ecCb(nil, classifyError(result.Ops[0].Err))
	}

	var meta *transactionDocMeta
	if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
		return ecCb(nil, classifyError(err))
	}

	var txnMeta *jsonTxnXattr
	if result.Ops[1].Err == nil {
		// Doc is currently in a txn.
		var txnMetaVal jsonTxnXattr
		if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
			return ecCb(nil, classifyError(err))
		}

		txnMeta = &txnMetaVal
	}

	var docBody []byte
	if result.Ops[2].Err == nil {
		docBody = result.Ops[2].Value
	}

	return &transactionGetDoc{
		Body:    docBody,
		TxnMeta: txnMeta,
		DocMeta: meta,
		Cas:     result.Cas,
		Deleted: result.DocIsDeleted,
	}, nil
}
