package transactionsx

import (
	"context"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type LostTransactionCleaner struct {
	logger            *zap.Logger
	atrAgent          *gocbcorex.Agent
	atrOboUser        string
	atrScopeName      string
	atrCollectionName string
	numAtrs           int
	cleanupWindow     time.Duration
	agentProvider     AgentProvider
	cleanupHooks      TransactionCleanupHooks
	clientRecordHooks TransactionClientRecordHooks

	uuid    string
	cleaner *TransactionCleaner
}

type LostTransactionCleanerConfig struct {
	Logger            *zap.Logger
	AtrAgent          *gocbcorex.Agent
	AtrOboUser        string
	AtrScopeName      string
	AtrCollectionName string
	NumATRS           int
	CleanupWindow     time.Duration
	AgentProvider     AgentProvider
	CleanupHooks      TransactionCleanupHooks
	ClientRecordHooks TransactionClientRecordHooks
}

func NewLostTransactionCleaner(config *LostTransactionCleanerConfig) *LostTransactionCleaner {
	cleanerUuid := uuid.New().String()

	cleaner := NewTransactionCleaner(&TransactionCleanerConfig{
		Logger: config.Logger,
		Hooks:  config.CleanupHooks,
	})

	return &LostTransactionCleaner{
		logger:            config.Logger,
		atrAgent:          config.AtrAgent,
		atrOboUser:        config.AtrOboUser,
		atrScopeName:      config.AtrScopeName,
		atrCollectionName: config.AtrCollectionName,
		numAtrs:           config.NumATRS,
		cleanupWindow:     config.CleanupWindow,
		agentProvider:     config.AgentProvider,
		cleanupHooks:      config.CleanupHooks,
		clientRecordHooks: config.ClientRecordHooks,

		uuid:    cleanerUuid,
		cleaner: cleaner,
	}
}

func (c *LostTransactionCleaner) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		err := c.cleanup(ctx)
		if err != nil {
			c.logger.Debug("cleanup failed",
				zap.Error(err))

			// TODO(brett19): Need to propagate the collection no longer existing up to
			// the manager thats running this, so that we can properly remove it from the
			// list of collections being processed.
		}
	}

	return ctx.Err()
}

func (c *LostTransactionCleaner) cleanup(ctx context.Context) error {
	c.logger.Debug("running cleanup", zap.String("uuid", c.uuid),
		zap.String("bucket", c.atrAgent.BucketName()),
		zap.String("scope", c.atrScopeName),
		zap.String("collection", c.atrCollectionName))

	atrsToProcess, err := c.processClient(ctx)
	if err != nil {
		return err
	}

	nextAtrTime := time.Now()
	timePerAtr := c.cleanupWindow / time.Duration(len(atrsToProcess))

	for _, atrId := range atrsToProcess {
		c.logger.Debug("processing atr",
			zap.String("uuid", c.uuid),
			zap.String("atrId", atrId))

		err := c.processAtr(ctx, []byte(atrId))
		if err != nil {
			c.logger.Debug("failed to process atr",
				zap.Error(err),
				zap.String("uuid", c.uuid),
				zap.String("atrId", atrId))
		}

		nextAtrTime = nextAtrTime.Add(timePerAtr)
		select {
		case <-time.After(time.Until(nextAtrTime)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
