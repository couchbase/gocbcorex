package transactionsx

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex"
)

type AgentProvider func(ctx context.Context, bucketName string) (*gocbcorex.Agent, string, error)

type LostCleanupLocation struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	NumATRs        int
}

type lostCleanupCleaner struct {
	location LostCleanupLocation
	cleaner  *LostTransactionCleaner

	bgCtx       context.Context
	bgCtxCancel context.CancelFunc
}

type LostTransactionCleanerManager struct {
	cleanupWindow     time.Duration
	cleaner           TransactionCleaner
	agentProvider     AgentProvider
	cleanupHooks      TransactionCleanupHooks
	clientRecordHooks TransactionClientRecordHooks

	lock         sync.Mutex
	closed       bool
	closeWaitGrp sync.WaitGroup
	cleaners     []*lostCleanupCleaner
}

type LostTransactionCleanerManagerConfig struct {
	ATRLocations      []LostCleanupLocation
	NumATRS           int
	CleanupWindow     time.Duration
	AgentProvider     AgentProvider
	Cleaner           TransactionCleaner
	CleanupHooks      TransactionCleanupHooks
	ClientRecordHooks TransactionClientRecordHooks
}

func (c *LostTransactionCleanerManager) AddLocation(loc LostCleanupLocation) {
	c.lock.Lock()

	if c.closed {
		c.lock.Unlock()
		return
	}

	// if the location is already in the list, return
	for _, cleaner := range c.cleaners {
		oloc := cleaner.location

		if loc.BucketName == oloc.BucketName &&
			loc.ScopeName == oloc.ScopeName &&
			loc.CollectionName == oloc.CollectionName {
			c.lock.Unlock()
			return
		}
	}

	bgCtx, bgCtxCancel := context.WithCancel(context.Background())
	cleanerEntry := &lostCleanupCleaner{
		location:    loc,
		bgCtx:       bgCtx,
		bgCtxCancel: bgCtxCancel,
	}

	c.closeWaitGrp.Add(1)
	c.cleaners = append(c.cleaners, cleanerEntry)

	c.lock.Unlock()

	go func() {
		defer c.closeWaitGrp.Done()

		agent, oboUser, err := c.agentProvider(bgCtx, loc.BucketName)
		if err != nil {
			// TODO(brett19): Handle this error...
			// failed to start lost cleanup thread
			return
		}

		// create a new cleaner
		newCleaner := NewLostTransactionCleaner(&LostTransactionCleanerConfig{
			AtrAgent:          agent,
			AtrOboUser:        oboUser,
			AtrScopeName:      loc.ScopeName,
			AtrCollectionName: loc.CollectionName,
			NumATRS:           loc.NumATRs,
			CleanupWindow:     c.cleanupWindow,
			AgentProvider:     c.agentProvider,
			Cleaner:           c.cleaner,
			CleanupHooks:      c.cleanupHooks,
			ClientRecordHooks: c.clientRecordHooks,
		})

		c.lock.Lock()
		cleanerEntry.cleaner = newCleaner
		c.lock.Unlock()

		err = newCleaner.Run(bgCtx)
		if err != nil {
			// TODO(brett19): handle this error
			return
		}

		// once we are done, remove this cleaner from the list
		c.lock.Lock()
		newCleaners := make([]*lostCleanupCleaner, 0, len(c.cleaners)-1)
		for _, cleaner := range c.cleaners {
			if cleaner != cleanerEntry {
				newCleaners = append(newCleaners, cleaner)
				break
			}
		}
		c.cleaners = newCleaners
		c.lock.Unlock()
	}()
}

func (c *LostTransactionCleanerManager) Close() {
	c.lock.Lock()

	c.closed = true

	for _, cleaner := range c.cleaners {
		cleaner.bgCtxCancel()
	}

	c.lock.Unlock()

	c.closeWaitGrp.Wait()
}
