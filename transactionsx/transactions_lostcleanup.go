package transactionsx

import (
	"context"
	"log"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/google/uuid"
)

type LostTransactionCleaner struct {
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
	return &LostTransactionCleaner{
		uuid:              uuid.New().String(),
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
			log.Printf("SCHED: Cleanup failed: %v", err)

			// TODO(brett19): Need to propagate the collection no longer existing up to
			// the manager thats running this, so that we can properly remove it from the
			// list of collections being processed.
		}
	}

	return ctx.Err()
}

func (c *LostTransactionCleaner) cleanup(ctx context.Context) error {
	log.Printf("SCHED: Running cleanup %s on %s.%s.%s", c.uuid, c.atrAgent.BucketName(), c.atrScopeName, c.atrCollectionName)

	atrsToProcess, err := c.processClient(ctx)
	if err != nil {
		return err
	}

	nextAtrTime := time.Now()
	timePerAtr := c.cleanupWindow / time.Duration(len(atrsToProcess))

	for _, atrId := range atrsToProcess {
		log.Printf("SCHED: Processing ATR %s", atrId)

		err := c.processAtr(ctx, []byte(atrId))
		if err != nil {
			log.Printf("SCHED: Failed to process ATR %s: %s", atrId, err)
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
