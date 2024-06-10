package transactionsx_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/couchbase/gocbcorex/transactionsx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createDefaultAgentOptions() gocbcorex.AgentOptions {
	logger, _ := zap.NewDevelopment()

	return gocbcorex.AgentOptions{
		Logger:     logger,
		TLSConfig:  nil,
		BucketName: testutilsint.TestOpts.BucketName,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: testutilsint.TestOpts.HTTPAddrs,
			MemdAddrs: testutilsint.TestOpts.MemdAddrs,
		},
		CompressionConfig: gocbcorex.CompressionConfig{
			EnableCompression: true,
		},
	}
}

func createDefaultAgent(t *testing.T) *gocbcorex.Agent {
	opts := createDefaultAgentOptions()

	agent, err := gocbcorex.CreateAgent(context.Background(), opts)
	require.NoError(t, err)

	return agent
}

func initTransactionAndAttempt(
	t *testing.T,
	agent *gocbcorex.Agent,
) (*transactionsx.TransactionsManager, *transactionsx.Transaction) {
	txns, err := transactionsx.InitTransactions(&transactionsx.TransactionsConfig{
		DurabilityLevel: transactionsx.TransactionDurabilityLevelNone,
		BucketAgentProvider: func(bucketName string) (*gocbcorex.Agent, string, error) {
			// We can always return just this one agent as we only actually
			// use a single bucket for this entire test.
			return agent, "", nil
		},
		ExpirationTime: 60 * time.Second,
	})
	require.NoError(t, err)

	txn, err := txns.BeginTransaction(nil)
	require.NoError(t, err)

	// Start the attempt
	err = txn.NewAttempt()
	require.NoError(t, err)

	return txns, txn
}

func fetchStagedOpData(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
) ([]byte, bool) {
	ctx := context.Background()

	result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key:            key,
		ScopeName:      "_default",
		CollectionName: "_default",
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("txn.op.stgd"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags: memdx.SubdocDocFlagAccessDeleted,
	})
	require.NoError(t, err)

	stgdOp := result.Ops[0]
	require.NoError(t, stgdOp.Err)

	stgdData := stgdOp.Value

	return stgdData, !result.DocIsDeleted
}

func assertStagedDoc(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
	expStgdData []byte,
	expTombstone bool,
) {
	stgdData, docExists := fetchStagedOpData(t, agent, key)

	assert.Equal(t, expStgdData, stgdData)
	assert.Equal(t, expTombstone, !docExists)
}

func assertDocNotStaged(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
) {
	ctx := context.Background()

	result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key:            key,
		ScopeName:      "_default",
		CollectionName: "_default",
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeExists,
				Path:  []byte("txn"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags: memdx.SubdocDocFlagAccessDeleted,
	})
	require.NoError(t, err)

	existsOp := result.Ops[0]
	assert.ErrorIs(t, existsOp.Err, memdx.ErrSubDocPathNotFound)
}

func TestTransactionsInsertTxn1GetTxn2(t *testing.T) {
	ctx := context.Background()

	agent := createDefaultAgent(t)
	defer agent.Close()

	_, txn := initTransactionAndAttempt(t, agent)

	docKey := []byte(fmt.Sprintf("%s-txnInsert", uuid.NewString()))
	docValue := []byte(`{"name":"mike"}`)

	_, err := txn.Insert(ctx, transactionsx.TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          docValue,
	})
	require.NoError(t, err)

	_, readTxn := initTransactionAndAttempt(t, agent)

	_, err = readTxn.Get(ctx, transactionsx.TransactionGetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	assert.ErrorIs(t, err, transactionsx.ErrDocNotFound)

	assertStagedDoc(t, agent, docKey, docValue, true)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	assertDocNotStaged(t, agent, docKey)

	getRes, err := readTxn.Get(ctx, transactionsx.TransactionGetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)
	assert.Equal(t, docValue, getRes.Value)
}
