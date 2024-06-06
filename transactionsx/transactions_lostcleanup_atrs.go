package transactionsx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

func (c *LostTransactionCleaner) processAtr(ctx context.Context, atrId []byte) error {
	cleanupReqs, err := c.fetchAtrExpiredAttempts(ctx, atrId)
	if err != nil {
		c.logger.Debug("failed to fetch atr for cleanup",
			zap.Error(err),
			zap.ByteString("atrId", atrId),
			zap.String("uuid", c.uuid))
		return nil
	}

	for _, req := range cleanupReqs {
		err := c.cleaner.CleanupAttempt(ctx, req)
		if err != nil {
			c.logger.Debug("failed to cleanup attempt",
				zap.Error(err),
				zap.String("attemptId", req.AttemptID),
				zap.ByteString("atrId", atrId),
				zap.String("uuid", c.uuid))
		}
	}

	return nil
}

func (c *LostTransactionCleaner) fetchAtrExpiredAttempts(ctx context.Context, atrId []byte) ([]*TransactionCleanupRequest, error) {
	err := c.cleanupHooks.BeforeATRGet(ctx, atrId)
	if err != nil {
		return nil, err
	}

	result, err := c.atrAgent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key: atrId,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("attempts"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  memdx.SubdocXattrPathHLC,
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		CollectionName: c.atrCollectionName,
		ScopeName:      c.atrScopeName,
		OnBehalfOf:     c.atrOboUser,
	})
	if err != nil {
		return nil, err
	}

	attemptsOp := result.Ops[0]
	if attemptsOp.Err != nil {
		return nil, attemptsOp.Err
	}

	hlcOp := result.Ops[1]
	if hlcOp.Err != nil {
		return nil, hlcOp.Err
	}

	var attempts map[string]jsonAtrAttempt
	err = json.Unmarshal(attemptsOp.Value, &attempts)
	if err != nil {
		return nil, err
	}

	hlcNow, err := memdx.ParseHLCToTime(hlcOp.Value)
	if err != nil {
		return nil, err
	}

	var attemptsToCleanup []*TransactionCleanupRequest
	for attemptId, attempt := range attempts {
		pendingCas, err := memdx.ParseMacroCasToCas([]byte(attempt.PendingCAS))
		if err != nil {
			return nil, err
		}

		hlcStartTime, err := memdx.ParseCasToTime(pendingCas)
		if err != nil {
			return nil, err
		}

		hlcExpiryTime := hlcStartTime.Add(time.Duration(attempt.ExpiryTimeNanos))
		if hlcNow.Before(hlcExpiryTime) {
			// skip transactions that have not expired yet
			continue
		}

		state, err := transactionsStateFromJson(attempt.State)
		if err != nil {
			return nil, err
		}

		durabilityLevel := transactionsDurabilityLevelFromJson(attempt.DurabilityLevel)

		parseAtrMutation := func(staged jsonAtrMutation) (TransactionCleanupDocRecord, error) {
			agent, oboUser, err := c.agentProvider(ctx, staged.BucketName)
			if err != nil {
				return TransactionCleanupDocRecord{}, err
			}

			return TransactionCleanupDocRecord{
				Agent:          agent,
				OboUser:        oboUser,
				CollectionName: staged.CollectionName,
				ScopeName:      staged.ScopeName,
			}, nil
		}

		var insertMutations []TransactionCleanupDocRecord
		for _, staged := range attempt.Inserts {
			docRecord, err := parseAtrMutation(staged)
			if err != nil {
				return nil, err
			}

			insertMutations = append(insertMutations, docRecord)
		}

		var replaceMutations []TransactionCleanupDocRecord
		for _, staged := range attempt.Replaces {
			docRecord, err := parseAtrMutation(staged)
			if err != nil {
				return nil, err
			}

			replaceMutations = append(replaceMutations, docRecord)
		}

		var removeMutations []TransactionCleanupDocRecord
		for _, staged := range attempt.Removes {
			docRecord, err := parseAtrMutation(staged)
			if err != nil {
				return nil, err
			}

			removeMutations = append(removeMutations, docRecord)
		}

		attemptsToCleanup = append(attemptsToCleanup, &TransactionCleanupRequest{
			AttemptID: attemptId,

			AtrAgent:          c.atrAgent,
			AtrOboUser:        c.atrOboUser,
			AtrScopeName:      c.atrScopeName,
			AtrCollectionName: c.atrCollectionName,
			AtrID:             atrId,

			Inserts:         insertMutations,
			Replaces:        replaceMutations,
			Removes:         removeMutations,
			State:           state,
			ForwardCompat:   forwardCompatFromJson(attempt.ForwardCompat),
			DurabilityLevel: durabilityLevel,

			// This is technically not correct, as we are conflating the local systems time with the HLC time.
			// However this is primarily used for logging and debugging purposes, so it should be fine.
			TxnStartTime: hlcStartTime,
		})
	}

	return attemptsToCleanup, nil
}
