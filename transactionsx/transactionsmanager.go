// Copyright 2021 Couchbase
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transactionsx

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// TransactionsManager is the top level wrapper object for all transactions
// handling.  It also manages the cleanup process in the background.
type TransactionsManager struct {
	config TransactionsConfig
}

// InitTransactions will initialize the transactions library and return a TransactionsManager
// object which can be used to perform transactions.
func InitTransactions(config *TransactionsConfig) (*TransactionsManager, error) {
	defaultConfig := TransactionsConfig{
		ExpirationTime:        10000 * time.Millisecond,
		DurabilityLevel:       DurabilityLevelMajority,
		CleanupWindow:         60000 * time.Millisecond,
		CleanupClientAttempts: true,
		CleanupLostAttempts:   true,
		BucketAgentProvider: func(bucketName string) (*gocbcorex.Agent, string, error) {
			return nil, "", errors.New("no bucket agent provider was specified")
		},
	}

	var resolvedConfig TransactionsConfig
	if config == nil {
		resolvedConfig = defaultConfig
	} else {
		resolvedConfig = *config

		if resolvedConfig.ExpirationTime == 0 {
			resolvedConfig.ExpirationTime = defaultConfig.ExpirationTime
		}
		if resolvedConfig.CleanupWindow == 0 {
			resolvedConfig.CleanupWindow = defaultConfig.CleanupWindow
		}
		if resolvedConfig.BucketAgentProvider == nil {
			resolvedConfig.BucketAgentProvider = defaultConfig.BucketAgentProvider
		}
		if resolvedConfig.CleanupQueueSize == 0 {
			resolvedConfig.CleanupQueueSize = 100000
		}
		if resolvedConfig.NumATRs == 0 {
			resolvedConfig.NumATRs = 1024
		}
	}

	if resolvedConfig.Logger == nil {
		resolvedConfig.Logger = zap.NewNop()
	}

	resolvedConfig.Logger.Info("Initializing transactions",
		zap.Any("config", config),
		zap.Any("resolvedConfig", resolvedConfig))

	t := &TransactionsManager{
		config: resolvedConfig,
	}

	return t, nil
}

// Config returns the config that was used during the initialization
// of this TransactionsManager object.
func (t *TransactionsManager) Config() TransactionsConfig {
	return t.config
}

// BeginTransaction will begin a new transaction.  The returned object can be used
// to begin a new attempt and subsequently perform operations before finally committing.
func (t *TransactionsManager) BeginTransaction(perConfig *TransactionOptions) (*Transaction, error) {
	transactionID := uuid.New().String()

	t.config.Logger.Info("Beginning transaction",
		zap.String("tid", transactionID),
		zap.Any("config", perConfig))

	expirationTime := t.config.ExpirationTime
	durabilityLevel := t.config.DurabilityLevel
	customATRLocation := t.config.CustomATRLocation
	bucketAgentProvider := t.config.BucketAgentProvider
	hooks := t.config.Hooks
	logger := t.config.Logger

	if perConfig != nil {
		if perConfig.ExpirationTime != 0 {
			expirationTime = perConfig.ExpirationTime
		}
		if perConfig.DurabilityLevel != DurabilityLevelUnknown {
			durabilityLevel = perConfig.DurabilityLevel
		}
		if perConfig.CustomATRLocation.Agent != nil {
			customATRLocation = perConfig.CustomATRLocation
		}
		if perConfig.BucketAgentProvider != nil {
			bucketAgentProvider = perConfig.BucketAgentProvider
		}
		if perConfig.Hooks != nil {
			hooks = *perConfig.Hooks
		}
		if perConfig.Logger != nil {
			logger = perConfig.Logger
		}
	}

	logger = logger.With(zap.String("tid", transactionID))

	now := time.Now()
	return &Transaction{
		expiryTime:              now.Add(expirationTime),
		startTime:               now,
		durabilityLevel:         durabilityLevel,
		transactionID:           transactionID,
		numAtrs:                 t.config.NumATRs,
		atrLocation:             customATRLocation,
		hooks:                   hooks,
		enableNonFatalGets:      t.config.EnableNonFatalGets,
		enableParallelUnstaging: t.config.EnableParallelUnstaging,
		enableExplicitATRs:      t.config.EnableExplicitATRs,
		enableMutationCaching:   t.config.EnableMutationCaching,
		bucketAgentProvider:     bucketAgentProvider,
		logger:                  logger,
	}, nil
}

// ResumeTransactionOptions specifies options which can be overridden for the resumed transaction.
type ResumeTransactionOptions struct {
	// BucketAgentProvider provides a function which returns an agent for
	// a particular bucket by name.
	BucketAgentProvider TransactionsBucketAgentProviderFn

	// Logger is the logger to use with this transaction.
	Logger *zap.Logger
}

// ResumeTransactionAttempt allows the resumption of an existing transaction attempt
// which was previously serialized, potentially by a different transaction client.
func (t *TransactionsManager) ResumeTransactionAttempt(txnBytes []byte, options *ResumeTransactionOptions) (*Transaction, error) {
	logger := t.config.Logger
	if options != nil {
		logger = options.Logger
	}

	bucketAgentProvider := t.config.BucketAgentProvider

	if options != nil {
		if options.BucketAgentProvider != nil {
			bucketAgentProvider = options.BucketAgentProvider
		}
	}

	var txnData SerializedAttemptJson
	err := json.Unmarshal(txnBytes, &txnData)
	if err != nil {
		return nil, err
	}

	if txnData.ID.Transaction == "" {
		return nil, errors.New("invalid txn data - no transaction id")
	}
	if txnData.Config.DurabilityLevel == "" {
		return nil, errors.New("invalid txn data - no durability level")
	}
	if txnData.State.TimeLeftMs <= 0 {
		return nil, errors.New("invalid txn data - time left must be greater than 0")
	}
	if txnData.Config.NumAtrs <= 0 || txnData.Config.NumAtrs > 1024 {
		return nil, errors.New("invalid txn data - num atrs must be greater than 0 and less than 1024")
	}

	var atrLocation ATRLocation
	if txnData.ATR.Bucket != "" && txnData.ATR.ID == "" {
		// ATR references the specific ATR for this transaction.

		foundAtrAgent, foundAtrOboUser, err := t.config.BucketAgentProvider(txnData.ATR.Bucket)
		if err != nil {
			return nil, err
		}

		atrLocation = ATRLocation{
			Agent:          foundAtrAgent,
			OboUser:        foundAtrOboUser,
			ScopeName:      txnData.ATR.Scope,
			CollectionName: txnData.ATR.Collection,
		}
	} else {
		// No ATR information means its pending with no custom.

		atrLocation = ATRLocation{
			Agent:          nil,
			OboUser:        "",
			ScopeName:      "",
			CollectionName: "",
		}
	}

	transactionID := txnData.ID.Transaction

	durabilityLevel, err := durabilityLevelFromString(txnData.Config.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	expirationTime := time.Duration(txnData.State.TimeLeftMs) * time.Millisecond

	logger = logger.With(zap.String("tid", transactionID))

	now := time.Now()
	txn := &Transaction{
		expiryTime:              now.Add(expirationTime),
		startTime:               now,
		durabilityLevel:         durabilityLevel,
		transactionID:           transactionID,
		atrLocation:             atrLocation,
		hooks:                   t.config.Hooks,
		enableNonFatalGets:      t.config.EnableNonFatalGets,
		enableParallelUnstaging: t.config.EnableParallelUnstaging,
		enableExplicitATRs:      t.config.EnableExplicitATRs,
		enableMutationCaching:   t.config.EnableMutationCaching,
		bucketAgentProvider:     bucketAgentProvider,
		logger:                  logger,
	}

	err = txn.resumeAttempt(&txnData)
	if err != nil {
		return nil, err
	}

	return txn, nil
}

// Close will shut down this TransactionsManager object, shutting down all
// background tasks associated with it.
func (t *TransactionsManager) Close() error {
	return nil
}

// CreateGetResultOptions exposes options for the CreateGetResult method.
type CreateGetResultOptions struct {
	Agent          *gocbcorex.Agent
	OboUser        string
	ScopeName      string
	CollectionName string
	Key            []byte
	Cas            uint64
	Meta           *MutableItemMeta
}

// CreateGetResult creates a false TransactionGetResult which can be used with Replace/Remove operations
// where the original TransactionGetResult is no longer available.
func CreateGetResult(opts CreateGetResultOptions) *GetResult {
	return &GetResult{
		agent:          opts.Agent,
		oboUser:        opts.OboUser,
		scopeName:      opts.ScopeName,
		collectionName: opts.CollectionName,
		key:            opts.Key,
		Meta:           opts.Meta,

		Value: nil,
		Cas:   opts.Cas,
	}
}
