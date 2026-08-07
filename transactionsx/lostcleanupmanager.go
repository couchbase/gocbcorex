package transactionsx

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/zaputils"

	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
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

type LostCleanupManager struct {
	logger            *zap.Logger
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

type LostCleanupManagerConfig struct {
	Logger            *zap.Logger
	ATRLocations      []LostCleanupLocation
	CleanupWindow     time.Duration
	AgentProvider     AgentProvider
	CleanupHooks      TransactionCleanupHooks
	ClientRecordHooks TransactionClientRecordHooks
}

func NewLostCleanupManager(config *LostCleanupManagerConfig) *LostCleanupManager {
	bgCtx, bgCtxCancel := context.WithCancel(context.Background())

	manager := &LostCleanupManager{
		logger:            config.Logger,
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

func (c *LostCleanupManager) AddLocation(loc LostCleanupLocation) {
	c.lock.Lock()

	c.logger.Debug("adding cleanup location",
		zaputils.FQCollectionName("location", loc.Agent.BucketName(), loc.ScopeName, loc.CollectionName))

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
		Logger:            c.logger,
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
			c.logger.Debug("cleanup location cleaner failed",
				zap.Error(err))
		}

		c.logger.Debug("completed cleanup location",
			zaputils.FQCollectionName("location", loc.Agent.BucketName(), loc.ScopeName, loc.CollectionName))

		// once we are done, remove this cleaner from the list
		c.lock.Lock()
		newCleaners := make([]*lostCleanupCleaner, 0, len(c.cleaners)-1)
		for _, cleaner := range c.cleaners {
			if cleaner != cleanerEntry {
				newCleaners = append(newCleaners, cleaner)
			}
		}
		c.cleaners = newCleaners
		c.lock.Unlock()
	}()
}

// Locations reports the ATR locations currently being watched.
func (c *LostCleanupManager) Locations() []LostCleanupLocation {
	c.lock.Lock()
	defer c.lock.Unlock()

	locations := make([]LostCleanupLocation, 0, len(c.cleaners))
	for _, cleaner := range c.cleaners {
		locations = append(locations, cleaner.location)
	}
	return locations
}

func (c *LostCleanupManager) Close() {
	c.lock.Lock()
	c.closed = true
	// Snapshot the cleaners: each removes itself from this list as its
	// goroutine winds down.
	cleaners := make([]*lostCleanupCleaner, len(c.cleaners))
	copy(cleaners, c.cleaners)
	c.lock.Unlock()

	c.bgCtxCancel()

	c.closeWaitGrp.Wait()

	// Drop out of each collection's client record now that we have stopped, so
	// the remaining clients pick up our ATRs without waiting for our heartbeat
	// to expire.  The background context is already cancelled by this point, so
	// these need a fresh one.
	for _, cleaner := range cleaners {
		err := cleaner.cleaner.RemoveClient(context.Background())
		if err != nil {
			c.logger.Debug("failed to remove client from client record",
				zap.Error(err),
				zaputils.FQCollectionName("location", cleaner.location.Agent.BucketName(),
					cleaner.location.ScopeName, cleaner.location.CollectionName))
		}
	}
}
