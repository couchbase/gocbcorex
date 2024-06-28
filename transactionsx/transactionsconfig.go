package transactionsx

import (
	"time"

	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
)

// ATRLocation specifies a specific location where ATR entries should be
// placed when performing transactions.
type ATRLocation struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
}

// LostATRLocation specifies a specific location where lost transactions should
// attempt cleanup.
type LostATRLocation struct {
	BucketName     string
	ScopeName      string
	CollectionName string
}

// TransactionsBucketAgentProviderFn is a function used to provide an agent for
// a particular bucket by name.
type TransactionsBucketAgentProviderFn func(bucketName string) (*gocbcorex.Agent, string, error)

// TransactionsConfig specifies various tunable options related to transactions.
type TransactionsConfig struct {
	// CustomATRLocation specifies a specific location to place meta-data.
	CustomATRLocation ATRLocation

	// ExpirationTime sets the maximum time that transactions created
	// by this TransactionsManager object can run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this TransactionsManager object.
	DurabilityLevel DurabilityLevel

	// CleanupWindow specifies how often to the cleanup process runs
	// attempting to garbage collection transactions that have failed but
	// were not cleaned up by the previous client.
	CleanupWindow time.Duration

	// CleanupClientAttempts controls where any transaction attempts made
	// by this client are automatically removed.
	CleanupClientAttempts bool

	// CleanupLostAttempts controls where a background process is created
	// to cleanup any ‘lost’ transaction attempts.
	CleanupLostAttempts bool

	// CleanupQueueSize controls the maximum queue size for the cleanup thread.
	CleanupQueueSize uint32

	// BucketAgentProvider provides a function which returns an agent for
	// a particular bucket by name.
	BucketAgentProvider TransactionsBucketAgentProviderFn

	// Logger specifies the logger to use for the transactions manager and for
	// any transaction which does not specify its own logger.
	Logger *zap.Logger

	// Hooks specifies hooks that can be configured to be invoked during execution.
	Hooks TransactionHooks

	// CleanupHooks specifies hooks related to the cleanup process.
	CleanUpHooks TransactionCleanupHooks

	// ClientRecordHooks specifies hooks related to client record management.
	ClientRecordHooks TransactionClientRecordHooks

	// EnableNonFatalGets controls whether non-fatal gets are enabled.
	EnableNonFatalGets bool

	// EnableParallelUnstaging controls whether parallel unstaging is enabled.
	EnableParallelUnstaging bool

	// EnableExplicitATRs controls whether explicit ATRs are required to be specified.
	EnableExplicitATRs bool

	// EnableMutationCaching controls whether mutation caching is enabled.
	EnableMutationCaching bool

	// NumATRs specifies the number of ATRs to use.
	NumATRs int
}

// TransactionOptions specifies options which can be overridden on a per transaction basis.
type TransactionOptions struct {
	// CustomATRLocation specifies a specific location to place meta-data.
	CustomATRLocation ATRLocation

	// ExpirationTime sets the maximum time that this transaction will
	// run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel DurabilityLevel

	// BucketAgentProvider provides a function which returns an agent for
	// a particular bucket by name.
	BucketAgentProvider TransactionsBucketAgentProviderFn

	// Logger is the logger to use with this transaction.
	Logger *zap.Logger

	// Hooks specifies hooks that can be configured to be invoked during
	// transaction execution
	Hooks TransactionHooks
}
