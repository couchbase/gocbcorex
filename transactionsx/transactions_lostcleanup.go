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
	uuid              string
	atrAgent          *gocbcorex.Agent
	atrOboUser        string
	atrScopeName      string
	atrCollectionName string
	numAtrs           int
	cleanupWindow     time.Duration
	cleaner           TransactionCleaner
	agentProvider     AgentProvider
	cleanupHooks      TransactionCleanupHooks
	clientRecordHooks TransactionClientRecordHooks
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
	Cleaner           TransactionCleaner
	CleanupHooks      TransactionCleanupHooks
	ClientRecordHooks TransactionClientRecordHooks
}

func NewLostTransactionCleaner(config *LostTransactionCleanerConfig) *LostTransactionCleaner {
	cleanerUuid := uuid.New().String()

	return &LostTransactionCleaner{
		logger:            config.Logger,
		uuid:              cleanerUuid,
		atrAgent:          config.AtrAgent,
		atrOboUser:        config.AtrOboUser,
		atrScopeName:      config.AtrScopeName,
		atrCollectionName: config.AtrCollectionName,
		numAtrs:           config.NumATRS,
		cleanupWindow:     config.CleanupWindow,
		agentProvider:     config.AgentProvider,
		cleaner:           config.Cleaner,
		cleanupHooks:      config.CleanupHooks,
		clientRecordHooks: config.ClientRecordHooks,
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
