package transactionsx

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex"
)

type AgentProvider func(ctx context.Context, bucketName string) (*gocbcorex.Agent, string, error)

type LostCleanupLocation struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	NumATRs        int
}

type lostCleanupCleaner struct {
	location LostCleanupLocation
	cleaner  *LostTransactionCleaner
}

type LostTransactionCleanerManager struct {
	cleanupWindow     time.Duration
	agentProvider     AgentProvider
	cleanupHooks      TransactionCleanupHooks
	clientRecordHooks TransactionClientRecordHooks

	lock         sync.Mutex
	closed       bool
	bgCtx        context.Context
	bgCtxCancel  context.CancelFunc
	closeWaitGrp sync.WaitGroup
	cleaners     []*lostCleanupCleaner
}

type LostTransactionCleanerManagerConfig struct {
	ATRLocations      []LostCleanupLocation
	CleanupWindow     time.Duration
	AgentProvider     AgentProvider
	CleanupHooks      TransactionCleanupHooks
	ClientRecordHooks TransactionClientRecordHooks
}

func NewLostTransactionCleanerManager(config *LostTransactionCleanerManagerConfig) *LostTransactionCleanerManager {
	bgCtx, bgCtxCancel := context.WithCancel(context.Background())

	manager := &LostTransactionCleanerManager{
		cleanupWindow:     config.CleanupWindow,
		agentProvider:     config.AgentProvider,
		cleanupHooks:      config.CleanupHooks,
		clientRecordHooks: config.ClientRecordHooks,

		bgCtx:       bgCtx,
		bgCtxCancel: bgCtxCancel,
	}

	for _, loc := range config.ATRLocations {
		manager.AddLocation(loc)
	}

	return manager
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

		if loc.Agent.BucketName() == oloc.Agent.BucketName() &&
			loc.OboUser == oloc.OboUser &&
			loc.ScopeName == oloc.ScopeName &&
			loc.CollectionName == oloc.CollectionName {
			c.lock.Unlock()
			return
		}
	}

	// create a new cleaner
	cleaner := NewLostTransactionCleaner(&LostTransactionCleanerConfig{
		AtrAgent:          loc.Agent,
		AtrOboUser:        loc.OboUser,
		AtrScopeName:      loc.ScopeName,
		AtrCollectionName: loc.CollectionName,
		NumATRS:           loc.NumATRs,
		CleanupWindow:     c.cleanupWindow,
		AgentProvider:     c.agentProvider,
		CleanupHooks:      c.cleanupHooks,
		ClientRecordHooks: c.clientRecordHooks,
	})

	cleanerEntry := &lostCleanupCleaner{
		location: loc,
		cleaner:  cleaner,
	}

	c.closeWaitGrp.Add(1)
	c.cleaners = append(c.cleaners, cleanerEntry)

	c.lock.Unlock()

	go func() {
		defer c.closeWaitGrp.Done()

		err := cleaner.Run(c.bgCtx)
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
	c.lock.Unlock()

	c.bgCtxCancel()

	c.closeWaitGrp.Wait()
}
