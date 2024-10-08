package transactionsx

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/zaputils"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
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
			if errors.Is(err, memdx.ErrUnknownCollectionName) {
				c.logger.Debug("cleanup failed due to unknown collection name, stopping",
					zap.Error(err))
				break
			}

			c.logger.Debug("cleanup failed, trying again",
				zap.Error(err))
			continue
		}
	}

	return ctx.Err()
}

func (c *LostTransactionCleaner) cleanup(ctx context.Context) error {
	c.logger.Debug("running cleanup",
		zap.String("uuid", c.uuid),
		zaputils.FQCollectionName("collection", c.atrAgent.BucketName(), c.atrScopeName, c.atrCollectionName))

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
