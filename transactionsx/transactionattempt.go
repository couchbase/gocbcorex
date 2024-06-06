package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/zaputils"
	"go.uber.org/zap"
)

type transactionAttempt struct {
	// immutable state
	expiryTime              time.Time
	txnStartTime            time.Time
	keyValueTimeout         time.Duration
	durabilityLevel         TransactionDurabilityLevel
	transactionID           string
	id                      string
	hooks                   TransactionHooks
	enableNonFatalGets      bool
	enableParallelUnstaging bool
	enableExplicitATRs      bool
	enableMutationCaching   bool
	atrLocation             TransactionATRLocation
	bucketAgentProvider     TransactionsBucketAgentProviderFn
	cleanupQueue            *TransactionCleanupQueue
	lostCleanupSystem       *TransactionsLostCleanupSystem

	// TODO(brett19): cleanupQueue here should probably be a cleanup interface.

	// mutable state
	state             TransactionAttemptState
	stateBits         uint32
	stagedMutations   []*transactionStagedMutation
	atrAgent          *gocbcorex.Agent
	atrOboUser        string
	atrScopeName      string
	atrCollectionName string
	atrKey            []byte

	lock sync.Mutex

	atrWaitCh     chan struct{}
	numPendingOps uint32
	opsWaitCh     chan struct{}

	logger *zap.Logger

	hasCleanupRequest bool

	/*
		addCleanupRequest      addCleanupRequest
		addLostCleanupLocation addLostCleanupLocation
	*/
}

func (t *transactionAttempt) State() TransactionAttemptResult {
	state := TransactionAttemptResult{}

	t.lock.Lock()

	stateBits := atomic.LoadUint32(&t.stateBits)

	state.State = t.state
	state.ID = t.id

	if stateBits&transactionStateBitHasExpired != 0 {
		state.Expired = true
	} else {
		state.Expired = false
	}

	if stateBits&transactionStateBitPreExpiryAutoRollback != 0 {
		state.PreExpiryAutoRollback = true
	} else {
		state.PreExpiryAutoRollback = false
	}

	if t.atrAgent != nil {
		state.AtrBucketName = t.atrAgent.BucketName()
		state.AtrScopeName = t.atrScopeName
		state.AtrCollectionName = t.atrCollectionName
		state.AtrID = t.atrKey
	} else {
		state.AtrBucketName = ""
		state.AtrScopeName = ""
		state.AtrCollectionName = ""
		state.AtrID = nil
	}

	if t.state == TransactionAttemptStateCompleted {
		state.UnstagingComplete = true
	} else {
		state.UnstagingComplete = false
	}

	t.lock.Unlock()

	return state
}

func (t *transactionAttempt) HasExpired() bool {
	return t.isExpiryOvertimeAtomic()
}

func (t *transactionAttempt) CanCommit() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotCommit) == 0
}

func (t *transactionAttempt) ShouldRollback() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotRollback) == 0
}

func (t *transactionAttempt) ShouldRetry() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits&transactionStateBitShouldNotRetry) == 0 && !t.isExpiryOvertimeAtomic()
}

func (t *transactionAttempt) FinalErrorToRaise() TransactionErrorReason {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return TransactionErrorReason((stateBits & transactionStateBitsMaskFinalError) >> transactionStateBitsPositionFinalError)
}

func (t *transactionAttempt) UpdateState(opts TransactionUpdateStateOptions) {
	t.logger.Info("updating state", zap.Stringer("opts", opts))

	stateBits := uint32(0)
	if opts.ShouldNotCommit {
		stateBits |= transactionStateBitShouldNotCommit
	}
	if opts.ShouldNotRollback {
		stateBits |= transactionStateBitShouldNotRollback
	}
	if opts.ShouldNotRetry {
		stateBits |= transactionStateBitShouldNotRetry
	}
	if opts.Reason == TransactionErrorReasonTransactionExpired {
		stateBits |= transactionStateBitHasExpired
	}
	t.applyStateBits(stateBits, uint32(opts.Reason))

	t.lock.Lock()
	if opts.State > 0 {
		t.state = opts.State
	}
	t.lock.Unlock()
}

func (t *transactionAttempt) GetATRLocation() TransactionATRLocation {
	t.lock.Lock()

	if t.atrAgent != nil {
		location := TransactionATRLocation{
			Agent:          t.atrAgent,
			ScopeName:      t.atrScopeName,
			CollectionName: t.atrCollectionName,
		}
		t.lock.Unlock()

		return location
	}
	t.lock.Unlock()

	return t.atrLocation
}

func (t *transactionAttempt) SetATRLocation(location TransactionATRLocation) error {
	t.logger.Info("setting atr location",
		zaputils.FQCollectionName("atr", t.atrAgent.BucketName(), t.atrScopeName, t.atrCollectionName))

	t.lock.Lock()
	if t.atrAgent != nil {
		t.lock.Unlock()
		return errors.New("atr location cannot be set after mutations have occurred")
	}

	if t.atrLocation.Agent != nil {
		t.lock.Unlock()
		return errors.New("atr location can only be set once")
	}

	t.atrLocation = location

	t.lock.Unlock()
	return nil
}

func (t *transactionAttempt) GetMutations() []TransactionStagedMutation {
	mutations := make([]TransactionStagedMutation, len(t.stagedMutations))

	t.lock.Lock()

	for mutationIdx, mutation := range t.stagedMutations {
		mutations[mutationIdx] = TransactionStagedMutation{
			OpType:         mutation.OpType,
			BucketName:     mutation.Agent.BucketName(),
			ScopeName:      mutation.ScopeName,
			CollectionName: mutation.CollectionName,
			Key:            mutation.Key,
			Cas:            mutation.Cas,
			Staged:         mutation.Staged,
		}
	}

	t.lock.Unlock()

	return mutations
}

func (t *transactionAttempt) TimeRemaining() time.Duration {
	curTime := time.Now()

	timeLeft := time.Duration(0)
	if curTime.Before(t.expiryTime) {
		timeLeft = t.expiryTime.Sub(curTime)
	}

	return timeLeft
}

func (t *transactionAttempt) toJsonObject() (jsonSerializedAttempt, error) {
	var res jsonSerializedAttempt

	t.lock.Lock()
	defer t.lock.Unlock()

	if err := t.checkCanCommitLocked(); err != nil {
		return res, err
	}

	res.ID.Transaction = t.transactionID
	res.ID.Attempt = t.id

	if t.atrAgent != nil {
		res.ATR.Bucket = t.atrAgent.BucketName()
		res.ATR.Scope = t.atrScopeName
		res.ATR.Collection = t.atrCollectionName
		res.ATR.ID = string(t.atrKey)
	} else if t.atrLocation.Agent != nil {
		res.ATR.Bucket = t.atrLocation.Agent.BucketName()
		res.ATR.Scope = t.atrLocation.ScopeName
		res.ATR.Collection = t.atrLocation.CollectionName
		res.ATR.ID = ""
	}

	res.Config.KeyValueTimeoutMs = int(t.keyValueTimeout / time.Millisecond)
	res.Config.DurabilityLevel = transactionDurabilityLevelToString(t.durabilityLevel)
	res.Config.NumAtrs = 1024

	res.State.TimeLeftMs = int(t.TimeRemaining().Milliseconds())

	for _, mutation := range t.stagedMutations {
		var mutationData jsonSerializedMutation

		mutationData.Bucket = mutation.Agent.BucketName()
		mutationData.Scope = mutation.ScopeName
		mutationData.Collection = mutation.CollectionName
		mutationData.ID = string(mutation.Key)
		mutationData.Cas = fmt.Sprintf("%d", mutation.Cas)
		mutationData.Type = transactionStagedMutationTypeToString(mutation.OpType)

		res.Mutations = append(res.Mutations, mutationData)
	}
	if len(res.Mutations) == 0 {
		res.Mutations = []jsonSerializedMutation{}
	}

	return res, nil
}

func (t *transactionAttempt) Serialize(ctx context.Context) ([]byte, error) {
	res, err := t.toJsonObject()
	if err != nil {
		return nil, err
	}

	resBytes, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}

	return resBytes, nil
}
