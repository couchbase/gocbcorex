package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/zaputils"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

// TransactionCleanupDocRecord represents an individual document operation requiring cleanup.
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
	ForwardCompat   map[string][]ForwardCompatEntry
	DurabilityLevel DurabilityLevel
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
	duraLevel := req.DurabilityLevel
	if duraLevel == DurabilityLevelUnknown {
		duraLevel = DurabilityLevelNone
	}
	memdDuraLevel, err := durabilityLevelToMemdx(duraLevel)
	if err != nil {
		return err
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
	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.fetchDoc(ctx, false, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	err = invokeNoResHookWithDocID(ctx, c.hooks.RemoveLinks, doc.ID, func() error {
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
			return err
		}

		return nil
	})
	if err != nil {
		c.logger.Debug("failed to rollback replace/remove",
			zap.Error(err),
			zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))

		return err
	}

	return nil
}

func (c *TransactionCleaner) rollbackInsDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.fetchDoc(ctx, false, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	if !getRes.Deleted {
		// legacy version that doesn't use shadow documents
		err = invokeNoResHookWithDocID(ctx, c.hooks.RemoveDoc, doc.ID, func() error {
			_, err := agent.Delete(ctx, &gocbcorex.DeleteOptions{
				Key:             doc.ID,
				ScopeName:       doc.ScopeName,
				CollectionName:  doc.CollectionName,
				Cas:             getRes.Cas,
				DurabilityLevel: durability,
				OnBehalfOf:      oboUser,
			})
			return err
		})
		if err != nil {
			c.logger.Debug("failed to rollback insert",
				zap.Error(err),
				zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))
			return err
		}

		return nil
	}

	err = invokeNoResHookWithDocID(ctx, c.hooks.RemoveDoc, doc.ID, func() error {
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
		return err
	})
	if err != nil {
		c.logger.Debug("failed to rollback shadow insert",
			zap.Error(err),
			zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))
		return err
	}

	return nil
}

func (c *TransactionCleaner) commitRemDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.fetchDoc(ctx, true, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	if getRes.TxnMeta.Operation.Type != MutationTypeJsonRemove {
		return nil
	}

	err = invokeNoResHookWithDocID(ctx, c.hooks.RemoveDocStagedForRemoval, doc.ID, func() error {
		_, err = agent.Delete(ctx, &gocbcorex.DeleteOptions{
			Key:             doc.ID,
			ScopeName:       doc.ScopeName,
			CollectionName:  doc.CollectionName,
			Cas:             getRes.Cas,
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		return err
	})
	if err != nil {
		c.logger.Debug("failed to commit replace/remove",
			zap.Error(err),
			zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))
		return err
	}

	return nil
}

func (c *TransactionCleaner) commitInsRepDoc(
	ctx context.Context,
	attemptID string,
	doc TransactionCleanupDocRecord,
	durability memdx.DurabilityLevel,
) error {
	agent, oboUser := doc.Agent, doc.OboUser

	getRes, err := c.fetchDoc(ctx, true, attemptID, doc, agent, oboUser)
	if err != nil {
		return err
	}

	if !getRes.Deleted {
		// legacy version that doesn't use shadow documents
		err = invokeNoResHookWithDocID(ctx, c.hooks.CommitDoc, doc.ID, func() error {
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
			return err
		})
		if err != nil {
			c.logger.Debug("failed to commit insert",
				zap.Error(err),
				zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))
			return err
		}
	}

	err = invokeNoResHookWithDocID(ctx, c.hooks.CommitDoc, doc.ID, func() error {
		_, err := agent.Add(ctx, &gocbcorex.AddOptions{
			Value:           getRes.Body,
			Key:             doc.ID,
			ScopeName:       doc.ScopeName,
			CollectionName:  doc.CollectionName,
			DurabilityLevel: durability,
			OnBehalfOf:      oboUser,
		})
		return err
	})
	if err != nil {
		c.logger.Debug("failed to commit shadow insert",
			zap.Error(err),
			zaputils.FQDocID("doc", doc.Agent.BucketName(), doc.ScopeName, doc.CollectionName, doc.ID))
		return err
	}

	return nil
}

type fetchDocResult struct {
	Body    []byte
	TxnMeta *TxnXattrJson
	DocMeta *TxnXattrDocMetaJson
	Cas     uint64
	Deleted bool
}

func (c *TransactionCleaner) fetchDoc(
	ctx context.Context,
	crc32MatchStaging bool,
	attemptID string,
	dr TransactionCleanupDocRecord,
	agent *gocbcorex.Agent,
	oboUser string,
) (*fetchDocResult, error) {
	zeroRes := &fetchDocResult{
		Body:    nil,
		TxnMeta: nil,
		DocMeta: &TxnXattrDocMetaJson{},
		Cas:     0,
		Deleted: true,
	}

	res, err := invokeHookWithDocID(ctx, c.hooks.DocGet, dr.ID, func() (*fetchDocResult, error) {
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

			return nil, err
		}

		if result.Ops[0].Err != nil {
			// This is not so good.
			return nil, result.Ops[0].Err
		}

		if result.Ops[1].Err != nil {
			// Txn probably committed so this is success.
			return zeroRes, nil
		}

		var txnMetaVal *TxnXattrJson
		if err := json.Unmarshal(result.Ops[1].Value, &txnMetaVal); err != nil {
			return nil, err
		}

		if attemptID != txnMetaVal.ID.Attempt {
			// Document involved in another txn, was probably committed, this is success.
			return zeroRes, nil
		}

		var meta *TxnXattrDocMetaJson
		if err := json.Unmarshal(result.Ops[0].Value, &meta); err != nil {
			return nil, err
		}

		if crc32MatchStaging {
			if meta.CRC32 != txnMetaVal.Operation.CRC32 {
				// This document is a part of this txn but its body has changed, we'll continue as success.
				return zeroRes, nil
			}
		}

		return &fetchDocResult{
			Body:    txnMetaVal.Operation.Staged,
			DocMeta: meta,
			Cas:     result.Cas,
			Deleted: result.DocIsDeleted,
			TxnMeta: txnMetaVal,
		}, nil
	})
	if err != nil {
		c.logger.Debug("failed to read document",
			zap.Error(err),
			zaputils.FQDocID("doc", dr.Agent.BucketName(), dr.ScopeName, dr.CollectionName, dr.ID))

		return nil, err
	}

	return res, nil
}

func (c *TransactionCleaner) cleanupATR(
	ctx context.Context,
	req *TransactionCleanupRequest,
) error {
	agent, oboUser := req.AtrAgent, req.AtrOboUser

	err := invokeNoResHookWithDocID(ctx, c.hooks.ATRRemove, req.AtrID, func() error {
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

		duraLevel := req.DurabilityLevel
		if duraLevel == DurabilityLevelUnknown {
			duraLevel = DurabilityLevelNone
		}
		memdDuraLevel, err := durabilityLevelToMemdx(duraLevel)
		if err != nil {
			return err
		}

		_, err = agent.MutateIn(ctx, &gocbcorex.MutateInOptions{
			Key:             req.AtrID,
			ScopeName:       req.AtrScopeName,
			CollectionName:  req.AtrCollectionName,
			Ops:             specs,
			DurabilityLevel: memdDuraLevel,
			OnBehalfOf:      oboUser,
		})
		return err
	})
	if err != nil {
		if errors.Is(err, memdx.ErrSubDocPathNotFound) {
			// if we can't find the ATR, we can assume it was already
			// cleaned up by someone else
			return nil
		}

		c.logger.Debug("failed to cleanup atr",
			zap.Error(err),
			zap.Any("req", req))

		return err
	}

	return nil
}

func (c *TransactionCleaner) checkForwardCompatability(
	stage forwardCompatStage,
	fc map[string][]ForwardCompatEntry,
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
