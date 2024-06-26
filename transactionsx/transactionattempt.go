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

type TransactionAttempt struct {
	// immutable state
	logger                  *zap.Logger
	expiryTime              time.Time
	txnStartTime            time.Time
	durabilityLevel         DurabilityLevel
	transactionID           string
	id                      string
	hooks                   TransactionHooks
	numAtrs                 int
	enableNonFatalGets      bool
	enableParallelUnstaging bool
	enableExplicitATRs      bool
	enableMutationCaching   bool
	atrLocation             ATRLocation
	bucketAgentProvider     TransactionsBucketAgentProviderFn
	cleanupQueue            *TransactionCleanupQueue
	lostCleanupSystem       *LostCleanupManager

	// mutable state
	state             TransactionAttemptState
	stateBits         uint32
	stagedMutations   []*stagedMutation
	atrAgent          *gocbcorex.Agent
	atrOboUser        string
	atrScopeName      string
	atrCollectionName string
	atrKey            []byte
	hasCleanupRequest bool
	numPendingOps     uint32
	atrWaitCh         chan struct{}
	opsWaitCh         chan struct{}

	lock sync.Mutex
}

func (t *TransactionAttempt) HasExpired() bool {
	return t.isExpiryOvertimeAtomic()
}

func (t *TransactionAttempt) CanCommit() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotCommit) == 0
}

func (t *TransactionAttempt) ShouldRetry() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits&transactionStateBitShouldNotRetry) == 0 && !t.isExpiryOvertimeAtomic()
}

func (t *TransactionAttempt) shouldRollback() bool {
	stateBits := atomic.LoadUint32(&t.stateBits)
	return (stateBits & transactionStateBitShouldNotRollback) == 0
}

func (t *TransactionAttempt) UpdateState(opts UpdateStateOptions) {
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
	t.applyStateBits(stateBits, opts.Reason)

	t.lock.Lock()
	if opts.State > 0 {
		t.state = opts.State
	}
	t.lock.Unlock()
}

func (t *TransactionAttempt) GetATRLocation() ATRLocation {
	t.lock.Lock()

	if t.atrAgent != nil {
		location := ATRLocation{
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

func (t *TransactionAttempt) SetATRLocation(location ATRLocation) error {
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

func (t *TransactionAttempt) GetMutations() []StagedMutation {
	mutations := make([]StagedMutation, len(t.stagedMutations))

	t.lock.Lock()

	for mutationIdx, mutation := range t.stagedMutations {
		mutations[mutationIdx] = StagedMutation{
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

func (t *TransactionAttempt) TimeRemaining() time.Duration {
	curTime := time.Now()

	timeLeft := time.Duration(0)
	if curTime.Before(t.expiryTime) {
		timeLeft = t.expiryTime.Sub(curTime)
	}

	return timeLeft
}

func (t *TransactionAttempt) toJsonObject() (SerializedAttemptJson, error) {
	var res SerializedAttemptJson

	t.lock.Lock()
	defer t.lock.Unlock()

	if errSt := t.checkCanCommitLocked(); errSt != nil {
		return res, errSt.Err()
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

	res.Config.DurabilityLevel = durabilityLevelToString(t.durabilityLevel)
	res.Config.NumAtrs = 1024

	// we set a static timeout here to ensure that transactions work with older clients
	// which still leverage this value, this isn't relevant for gocbcorex.
	res.Config.KeyValueTimeoutMs = 2500

	res.State.TimeLeftMs = int(t.TimeRemaining().Milliseconds())

	for _, mutation := range t.stagedMutations {
		var mutationData SerializedMutationJson

		mutationData.Bucket = mutation.Agent.BucketName()
		mutationData.Scope = mutation.ScopeName
		mutationData.Collection = mutation.CollectionName
		mutationData.ID = string(mutation.Key)
		mutationData.Cas = fmt.Sprintf("%d", mutation.Cas)
		mutationData.Type = stagedMutationTypeToString(mutation.OpType)

		res.Mutations = append(res.Mutations, mutationData)
	}
	if len(res.Mutations) == 0 {
		res.Mutations = []SerializedMutationJson{}
	}

	return res, nil
}

func (t *TransactionAttempt) Serialize(ctx context.Context) ([]byte, error) {
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
