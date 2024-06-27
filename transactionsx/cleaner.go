package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

// TransactionCleanupDocRecord represents an individual document operation requiring cleanup.
// Internal: This should never be used and is not supported.
type TransactionCleanupDocRecord struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	ID             []byte
}

type TransactionCleanupRequest struct {
	AttemptID string

	AtrAgent          *gocbcorex.Agent
	AtrOboUser        string
	AtrScopeName      string
	AtrCollectionName string
	AtrID             []byte

	Inserts         []TransactionCleanupDocRecord
	Replaces        []TransactionCleanupDocRecord
	Removes         []TransactionCleanupDocRecord
	State           TransactionAttemptState
	ForwardCompat   map[string][]TransactionForwardCompatibilityEntry
	DurabilityLevel TransactionDurabilityLevel
	TxnStartTime    time.Time
}

type TransactionCleaner struct {
	logger *zap.Logger
	hooks  TransactionCleanupHooks
}

type TransactionCleanerConfig struct {
	Logger *zap.Logger
	Hooks  TransactionCleanupHooks
}

func NewTransactionCleaner(config *TransactionCleanerConfig) *TransactionCleaner {
	return &TransactionCleaner{
		logger: config.Logger,
		hooks:  config.Hooks,
	}
}

func (c *TransactionCleaner) CleanupAttempt(
	ctx context.Context,
	req *TransactionCleanupRequest,
) error {
	c.logger.Debug("cleaning up attempt",
		zap.Any("req", req))

	err := c.cleanupAttempt(ctx, req)
	if err != nil {
		c.logger.Warn("cleanup attempt failed",
			zap.Error(err),
			zap.Any("req", req))

		txnAge := time.Since(req.TxnStartTime)
		if txnAge > 2*time.Hour {
			c.logger.Warn("cleanup request failed and is very old - please raise with support",
				zap.String("attemptID", req.AttemptID),
				zap.Duration("txnAge", txnAge))
		}

		return err
	}

	return nil
}

func (c *TransactionCleaner) cleanupAttempt(
	ctx context.Context,
	req *TransactionCleanupRequest,
) error {
	err := c.checkForwardCompatability(forwardCompatStageGetsCleanupEntry, req.ForwardCompat)
	if err != nil {
		return err
	}

	err = c.cleanupDocs(ctx, req)
	if err != nil {
		return err
	}

	err = c.cleanupATR(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (c *TransactionCleaner) cleanupDocs(
	ctx context.Context,
	req *TransactionCleanupRequest,
) error {
	var memdDuraLevel memdx.DurabilityLevel
	if req.DurabilityLevel > TransactionDurabilityLevelUnknown {
		// We want to ensure that we don't panic here, if the durability level is unknown then we'll just not set
		// a durability level.
		memdDuraLevel = transactionsDurabilityLevelToMemdx(req.DurabilityLevel)
	}

	switch req.State {
	case TransactionAttemptStateCommitted:
		for _, doc := range req.Inserts {
			err := c.commitInsRepDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		for _, doc := range req.Replaces {
			err := c.commitInsRepDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		for _, doc := range req.Removes {
			err := c.commitRemDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		return nil
	case TransactionAttemptStateAborted:
		for _, doc := range req.Inserts {
			err := c.rollbackInsDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		for _, doc := range req.Replaces {
			err := c.rollbackRepRemDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		for _, doc := range req.Removes {
			err := c.rollbackRepRemDoc(ctx, req.AttemptID, doc, memdDuraLevel)
			if err != nil {
				return err
			}
		}

		return nil
	case TransactionAttemptStatePending:
		return nil
	case TransactionAttemptStateCompleted:
		return nil
	case TransactionAttemptStateRolledBack:
		return nil
	case TransactionAttemptStateNothingWritten:
		return nil
	}

	return nil
}

func (c *TransactionCleaner) rollbackRepRemDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		return err
	}

	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.perDoc(ctx, false, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	err = c.hooks.BeforeRemoveLinks(ctx, doc.ID)
	if err != nil {
		return ecCb(err)
	}

	_, err = agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            doc.ID,
		ScopeName:      doc.ScopeName,
		CollectionName: doc.CollectionName,
		Cas:            getRes.Cas,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDelete,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags:           memdx.SubdocDocFlagAccessDeleted,
		DurabilityLevel: durability,
		OnBehalfOf:      oboUser,
	})
	if err != nil {
		c.logger.Debug("failed to rollback replace/remove",
			zap.Error(err),
			zap.String("bucket", doc.Agent.BucketName()),
			zap.String("collection", doc.CollectionName),
			zap.String("scope", doc.ScopeName),
			zap.ByteString("id", doc.ID))
		return ecCb(err)
	}

	return nil
}

func (c *TransactionCleaner) rollbackInsDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		return err
	}

	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.perDoc(ctx, false, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	err = c.hooks.BeforeRemoveDoc(ctx, doc.ID)
	if err != nil {
		return ecCb(err)
	}

	if getRes.Deleted {
		_, err := agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
			Key:            doc.ID,
			ScopeName:      doc.ScopeName,
			CollectionName: doc.CollectionName,
			Cas:            getRes.Cas,
			Ops: []memdx.MutateInOp{
				{
					Op:    memdx.MutateInOpTypeDelete,
					Path:  []byte("txn"),
					Flags: memdx.SubdocOpFlagXattrPath,
				},
			},
			Flags:           memdx.SubdocDocFlagAccessDeleted,
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		if err != nil {
			c.logger.Debug("failed to rollback shadow insert",
				zap.Error(err),
				zap.String("bucket", doc.Agent.BucketName()),
				zap.String("collection", doc.CollectionName),
				zap.String("scope", doc.ScopeName),
				zap.ByteString("id", doc.ID))
			return ecCb(err)
		}

		return nil
	} else {
		_, err := agent.Delete(ctx, &gocbcorex.DeleteOptions{
			Key:             doc.ID,
			ScopeName:       doc.ScopeName,
			CollectionName:  doc.CollectionName,
			Cas:             getRes.Cas,
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		if err != nil {
			c.logger.Debug("failed to rollback insert",
				zap.Error(err),
				zap.String("bucket", doc.Agent.BucketName()),
				zap.String("collection", doc.CollectionName),
				zap.String("scope", doc.ScopeName),
				zap.ByteString("id", doc.ID))
			return ecCb(err)
		}
	}

	return nil
}

func (c *TransactionCleaner) commitRemDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		return err
	}

	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.perDoc(ctx, true, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	err = c.hooks.BeforeRemoveDocStagedForRemoval(ctx, doc.ID)
	if err != nil {
		return ecCb(err)
	}

	if getRes.TxnMeta.Operation.Type != jsonMutationRemove {
		return nil
	}

	_, err = agent.Delete(ctx, &gocbcorex.DeleteOptions{
		Key:             doc.ID,
		ScopeName:       doc.ScopeName,
		CollectionName:  doc.CollectionName,
		Cas:             getRes.Cas,
		DurabilityLevel: durability,
		OnBehalfOf:      oboUser,
	})
	if err != nil {
		c.logger.Debug("failed to commit replace/remove",
			zap.Error(err),
			zap.String("bucket", doc.Agent.BucketName()),
			zap.String("collection", doc.CollectionName),
			zap.String("scope", doc.ScopeName),
			zap.ByteString("id", doc.ID))
		return ecCb(err)
	}

	return nil
}

func (c *TransactionCleaner) commitInsRepDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		return err
	}

	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.perDoc(ctx, true, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	err = c.hooks.BeforeCommitDoc(ctx, doc.ID)
	if err != nil {
		return ecCb(err)
	}

	if getRes.Deleted {
		_, err := agent.Add(ctx, &gocbcorex.AddOptions{
			Value:           getRes.Body,
			Key:             doc.ID,
			ScopeName:       doc.ScopeName,
			CollectionName:  doc.CollectionName,
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		if err != nil {
			c.logger.Debug("failed to commit shadow insert",
				zap.Error(err),
				zap.String("bucket", doc.Agent.BucketName()),
				zap.String("collection", doc.CollectionName),
				zap.String("scope", doc.ScopeName),
				zap.ByteString("id", doc.ID))
			return ecCb(err)
		}

		return nil
	} else {
		_, err := agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
			Key:            doc.ID,
			ScopeName:      doc.ScopeName,
			CollectionName: doc.CollectionName,
			Cas:            getRes.Cas,
			Ops: []memdx.MutateInOp{
				{
					Op:    memdx.MutateInOpTypeDelete,
					Path:  []byte("txn"),
					Flags: memdx.SubdocOpFlagXattrPath,
				},
				{
					Op:    memdx.MutateInOpTypeSetDoc,
					Path:  nil,
					Value: getRes.Body,
				},
			},
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		if err != nil {
			c.logger.Debug("failed to commit insert",
				zap.Error(err),
				zap.String("bucket", doc.Agent.BucketName()),
				zap.String("collection", doc.CollectionName),
				zap.String("scope", doc.ScopeName),
				zap.ByteString("id", doc.ID))
			return ecCb(err)
		}
	}

	return nil
}

func (c *TransactionCleaner) perDoc(
	ctx context.Context,
	crc32MatchStaging bool,
	attemptID string,
	dr TransactionCleanupDocRecord,
	agent *gocbcorex.Agent,
	oboUser string,
) (*transactionGetDoc, error) {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		return err
	}

	zeroRes := &transactionGetDoc{
		Body:    nil,
		TxnMeta: nil,
		DocMeta: &transactionDocMeta{},
		Cas:     0,
		Deleted: true,
	}

	err := c.hooks.BeforeDocGet(ctx, dr.ID)
	if err != nil {
		return nil, ecCb(err)
	}

	result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		ScopeName:      dr.ScopeName,
		CollectionName: dr.CollectionName,
		Key:            dr.ID,
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
		},
		Flags:      memdx.SubdocDocFlagAccessDeleted,
		OnBehalfOf: oboUser,
	})
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			// We can consider this success.
			return zeroRes, nil
		}

		c.logger.Debug("failed to read document",
			zap.Error(err),
			zap.String("bucket", agent.BucketName()),
			zap.String("collection", dr.CollectionName),
			zap.String("scope", dr.ScopeName),
			zap.ByteString("id", dr.ID))
		return nil, ecCb(err)
	}

	if result.Ops[0].Err != nil {
		// This is not so good.
		return nil, ecCb(result.Ops[0].Err)
	}

	if result.Ops[1].Err != nil {
		// Txn probably committed so this is success.
		return zeroRes, nil
	}

	var txnMetaVal *jsonTxnXattr
	if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
		return nil, ecCb(err)
	}

	if attemptID != txnMetaVal.ID.Attempt {
		// Document involved in another txn, was probably committed, this is success.
		return zeroRes, nil
	}

	var meta *transactionDocMeta
	if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
		return nil, ecCb(err)
	}

	if crc32MatchStaging {
		if meta.CRC32 != txnMetaVal.Operation.CRC32 {
			// This document is a part of this txn but its body has changed, we'll continue as success.
			return zeroRes, nil
		}
	}

	return &transactionGetDoc{
		Body:    txnMetaVal.Operation.Staged,
		DocMeta: meta,
		Cas:     result.Cas,
		Deleted: result.DocIsDeleted,
		TxnMeta: txnMetaVal,
	}, nil
}

func (c *TransactionCleaner) cleanupATR(
	ctx context.Context,
	req *TransactionCleanupRequest,
) error {
	ecCb := func(err error) error {
		if err == nil {
			return nil
		}

		if errors.Is(err, memdx.ErrSubDocPathNotFound) {
			return nil
		}

		c.logger.Debug("failed to cleanup atr",
			zap.Error(err),
			zap.Any("req", req))

		return err
	}

	agent, oboUser := req.AtrAgent, req.AtrOboUser

	err := c.hooks.BeforeATRRemove(ctx, req.AtrID)
	if err != nil {
		return ecCb(err)
	}

	var specs []memdx.MutateInOp
	if req.State == TransactionAttemptStatePending {
		specs = append(specs, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictAdd,
			Value: []byte("null"),
			Path:  []byte("attempts." + req.AttemptID + ".p"),
			Flags: memdx.SubdocOpFlagXattrPath,
		})
	}

	specs = append(specs, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeDelete,
		Path:  []byte("attempts." + req.AttemptID),
		Flags: memdx.SubdocOpFlagXattrPath,
	})

	var memdDuraLevel memdx.DurabilityLevel
	if req.DurabilityLevel > TransactionDurabilityLevelUnknown {
		memdDuraLevel = transactionsDurabilityLevelToMemdx(req.DurabilityLevel)
	}

	_, err = agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:             req.AtrID,
		ScopeName:       req.AtrScopeName,
		CollectionName:  req.AtrCollectionName,
		Ops:             specs,
		DurabilityLevel: memdDuraLevel,
		OnBehalfOf:      oboUser,
	})
	if err != nil {
		return ecCb(err)
	}

	return nil
}

func (c *TransactionCleaner) checkForwardCompatability(
	stage forwardCompatStage,
	fc map[string][]TransactionForwardCompatibilityEntry,
) error {
	isCompat, _, _, err := checkForwardCompatability(stage, fc)
	if err != nil {
		return err
	}

	if !isCompat {
		return ErrForwardCompatibilityFailure
	}

	return nil
}
