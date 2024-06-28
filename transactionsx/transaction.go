package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Transaction represents a single active transaction, it can be used to
// stage mutations and finally commit them.
type Transaction struct {
	expiryTime              time.Time
	startTime               time.Time
	durabilityLevel         DurabilityLevel
	numAtrs                 int
	enableParallelUnstaging bool
	enableNonFatalGets      bool
	enableExplicitATRs      bool
	enableMutationCaching   bool
	atrLocation             ATRLocation
	bucketAgentProvider     TransactionsBucketAgentProviderFn

	transactionID string
	attempt       *TransactionAttempt
	hooks         TransactionHooks
	logger        *zap.Logger
}

// ID returns the transaction ID of this transaction.
func (t *Transaction) ID() string {
	return t.transactionID
}

// NewAttempt begins a new attempt with this transaction.
func (t *Transaction) NewAttempt() error {
	attemptUUID := uuid.New().String()

	logger := t.logger
	logger = logger.With(zap.String("attemptId", attemptUUID))

	t.attempt = &TransactionAttempt{
		expiryTime:              t.expiryTime,
		txnStartTime:            t.startTime,
		durabilityLevel:         t.durabilityLevel,
		transactionID:           t.transactionID,
		numAtrs:                 t.numAtrs,
		enableNonFatalGets:      t.enableNonFatalGets,
		enableParallelUnstaging: t.enableParallelUnstaging,
		enableMutationCaching:   t.enableMutationCaching,
		enableExplicitATRs:      t.enableExplicitATRs,
		atrLocation:             t.atrLocation,
		bucketAgentProvider:     t.bucketAgentProvider,

		id:                attemptUUID,
		state:             TransactionAttemptStateNothingWritten,
		stagedMutations:   nil,
		atrAgent:          nil,
		atrScopeName:      "",
		atrCollectionName: "",
		atrKey:            nil,
		hooks:             t.hooks,
		logger:            logger,
	}

	return nil
}

func (t *Transaction) resumeAttempt(txnData *SerializedAttemptJson) error {
	if txnData.ID.Attempt == "" {
		return errors.New("invalid txn data - no attempt id")
	}

	attemptUUID := txnData.ID.Attempt

	var txnState TransactionAttemptState
	var atrAgent *gocbcorex.Agent
	var atrOboUser string
	var atrScope, atrCollection string
	var atrKey []byte
	if txnData.ATR.ID != "" {
		// ATR references the specific ATR for this transaction.

		if txnData.ATR.Bucket == "" {
			return errors.New("invalid atr data - no bucket")
		}

		foundAtrAgent, foundAtrOboUser, err := t.bucketAgentProvider(txnData.ATR.Bucket)
		if err != nil {
			return err
		}

		txnState = TransactionAttemptStatePending
		atrAgent = foundAtrAgent
		atrOboUser = foundAtrOboUser
		atrScope = txnData.ATR.Scope
		atrCollection = txnData.ATR.Collection
		atrKey = []byte(txnData.ATR.ID)
	} else {
		// No ATR information means its pending with no custom.

		txnState = TransactionAttemptStateNothingWritten
		atrAgent = nil
		atrOboUser = ""
		atrScope = ""
		atrCollection = ""
		atrKey = nil
	}

	stagedMutations := make([]*stagedMutation, len(txnData.Mutations))
	for mutationIdx, mutationData := range txnData.Mutations {
		if mutationData.Bucket == "" {
			return errors.New("invalid staged mutation - no bucket")
		}
		if mutationData.ID == "" {
			return errors.New("invalid staged mutation - no key")
		}
		if mutationData.Cas == "" {
			return errors.New("invalid staged mutation - no cas")
		}
		if mutationData.Type == "" {
			return errors.New("invalid staged mutation - no type")
		}

		agent, oboUser, err := t.bucketAgentProvider(mutationData.Bucket)
		if err != nil {
			return err
		}

		cas, err := strconv.ParseUint(mutationData.Cas, 10, 64)
		if err != nil {
			return err
		}

		opType, err := stagedMutationTypeFromString(mutationData.Type)
		if err != nil {
			return err
		}

		stagedMutations[mutationIdx] = &stagedMutation{
			OpType:         opType,
			Agent:          agent,
			OboUser:        oboUser,
			ScopeName:      mutationData.Scope,
			CollectionName: mutationData.Collection,
			Key:            []byte(mutationData.ID),
			Cas:            cas,
			Staged:         nil,
		}
	}

	t.attempt = &TransactionAttempt{
		expiryTime:              t.expiryTime,
		txnStartTime:            t.startTime,
		durabilityLevel:         t.durabilityLevel,
		transactionID:           t.transactionID,
		numAtrs:                 t.numAtrs,
		enableNonFatalGets:      t.enableNonFatalGets,
		enableParallelUnstaging: t.enableParallelUnstaging,
		enableMutationCaching:   t.enableMutationCaching,
		enableExplicitATRs:      t.enableExplicitATRs,
		atrLocation:             t.atrLocation,
		bucketAgentProvider:     t.bucketAgentProvider,

		id:                attemptUUID,
		state:             txnState,
		stagedMutations:   stagedMutations,
		atrAgent:          atrAgent,
		atrOboUser:        atrOboUser,
		atrScopeName:      atrScope,
		atrCollectionName: atrCollection,
		atrKey:            atrKey,
		hooks:             t.hooks,
	}

	return nil
}

// GetOptions provides options for a Get operation.
type GetOptions struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	Key            []byte

	// NoRYOW will disable the RYOW logic used to enable transactions
	// to naturally read any mutations they have performed.
	// VOLATILE: This parameter is subject to change.
	NoRYOW bool
}

// MutableItemMetaATR represents the ATR for meta.
type MutableItemMetaATR struct {
	BucketName     string `json:"bkt"`
	ScopeName      string `json:"scp"`
	CollectionName string `json:"coll"`
	DocID          string `json:"key"`
}

// MutableItemMeta represents all the meta-data for a fetched
// item.  Most of this is used for later mutation operations.
type MutableItemMeta struct {
	TransactionID string                          `json:"txn"`
	AttemptID     string                          `json:"atmpt"`
	ATR           MutableItemMetaATR              `json:"atr"`
	ForwardCompat map[string][]ForwardCompatEntry `json:"fc,omitempty"`
}

// GetResult represents the result of a Get or GetOptional operation.
type GetResult struct {
	agent          *gocbcorex.Agent
	oboUser        string
	scopeName      string
	collectionName string
	key            []byte

	Meta  *MutableItemMeta
	Value []byte
	Cas   uint64
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (t *Transaction) Get(ctx context.Context, opts GetOptions) (*GetResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Get(ctx, opts)
}

// InsertOptions provides options for a Insert operation.
type InsertOptions struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	Key            []byte
	Value          json.RawMessage
}

// Insert will attempt to insert a document.
func (t *Transaction) Insert(ctx context.Context, opts InsertOptions) (*GetResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Insert(ctx, opts)
}

// ReplaceOptions provides options for a Replace operation.
type ReplaceOptions struct {
	Document *GetResult
	Value    json.RawMessage
}

// Replace will attempt to replace an existing document.
func (t *Transaction) Replace(ctx context.Context, opts ReplaceOptions) (*GetResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Replace(ctx, opts)
}

// RemoveOptions provides options for a Remove operation.
type RemoveOptions struct {
	Document *GetResult
}

// Remove will attempt to remove a previously fetched document.
func (t *Transaction) Remove(ctx context.Context, opts RemoveOptions) (*GetResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Remove(ctx, opts)
}

// Commit will attempt to commit the transaction, rolling it back and cancelling
// it if it is not capable of doing so.
func (t *Transaction) Commit(ctx context.Context) (*TransactionAttemptResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Commit(ctx)
}

// Rollback will attempt to rollback the transaction.
func (t *Transaction) Rollback(ctx context.Context) (*TransactionAttemptResult, error) {
	if t.attempt == nil {
		return nil, ErrNoAttempt
	}

	return t.attempt.Rollback(ctx)
}

// HasExpired indicates whether this attempt has expired.
func (t *Transaction) HasExpired() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.HasExpired()
}

// CanCommit indicates whether this attempt can still be committed.
func (t *Transaction) CanCommit() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.CanCommit()
}

// ShouldRetry indicates if this attempt thinks we can retry.
func (t *Transaction) ShouldRetry() bool {
	if t.attempt == nil {
		return false
	}

	return t.attempt.ShouldRetry()
}

func (t *Transaction) TimeRemaining() time.Duration {
	if t.attempt == nil {
		return 0
	}

	return t.attempt.TimeRemaining()
}

// SerializeAttempt will serialize the current transaction attempt, allowing it
// to be resumed later, potentially under a different transactions client.  It
// is no longer safe to use this attempt once this has occurred, a new attempt
// must be started to use this object following this call.
func (t *Transaction) SerializeAttempt(ctx context.Context) ([]byte, error) {
	return t.attempt.Serialize(ctx)
}

// GetMutations returns a list of all the current mutations that have been performed
// under this transaction.
func (t *Transaction) GetMutations() []StagedMutation {
	if t.attempt == nil {
		return nil
	}

	return t.attempt.GetMutations()
}

// GetATRLocation returns the ATR location for the current attempt, either by
// identifying where it was placed, or where it will be based on custom atr
// configurations.
func (t *Transaction) GetATRLocation() ATRLocation {
	if t.attempt != nil {
		return t.attempt.GetATRLocation()
	}

	return t.atrLocation
}

// SetATRLocation forces the ATR location for the current attempt to a specific
// location.  Note that this cannot be called if it has already been set.  This
// is currently only safe to call before any mutations have occurred.
func (t *Transaction) SetATRLocation(location ATRLocation) error {
	if t.attempt == nil {
		return errors.New("cannot set ATR location without an active attempt")
	}

	return t.attempt.SetATRLocation(location)
}

// Config returns the configured parameters for this transaction.
// Note that the Expiration time is adjusted based on the time left.
// Note also that after a transaction is resumed, the custom atr location
// may no longer reflect the originally configured value.
func (t *Transaction) Config() TransactionOptions {
	return TransactionOptions{
		CustomATRLocation: t.atrLocation,
		ExpirationTime:    t.TimeRemaining(),
		DurabilityLevel:   t.durabilityLevel,
	}
}

// UpdateStateOptions are the settings available to UpdateState.
// This function must only be called once the transaction has entered query mode.
type UpdateStateOptions struct {
	ShouldNotCommit   bool
	ShouldNotRollback bool
	ShouldNotRetry    bool
	State             TransactionAttemptState
	Reason            TransactionErrorReason
}

func (tuso UpdateStateOptions) String() string {
	return fmt.Sprintf("Should not commit: %t, should not rollback: %t, should not retry: %t, state: %s, reason: %s",
		tuso.ShouldNotCommit, tuso.ShouldNotRollback, tuso.ShouldNotRetry, tuso.State, tuso.Reason)
}

// UpdateState will update the internal state of the current attempt.
func (t *Transaction) UpdateState(opts UpdateStateOptions) {
	if t.attempt == nil {
		return
	}

	t.attempt.UpdateState(opts)
}
