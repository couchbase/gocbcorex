package transactionsx_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/couchbase/gocbcorex/transactionsx"
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
) (string, []byte, bool) {
	ctx := context.Background()

	result, err := agent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key:            key,
		ScopeName:      "_default",
		CollectionName: "_default",
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("txn.op.type"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("txn.op.stgd"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		Flags: memdx.SubdocDocFlagAccessDeleted,
	})

	typeOp := result.Ops[0]
	require.NoError(t, typeOp.Err)

	stgdOp := result.Ops[1]
	require.NoError(t, stgdOp.Err)

	var opType string
	err = json.Unmarshal(typeOp.Value, &opType)
	if err != nil {
		return "", nil, false
	}

	stgdData := stgdOp.Value

	return opType, stgdData, !result.DocIsDeleted
}

func assertStagedDoc(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
	expOpType string,
	expStgdData []byte,
	expTombstone bool,
) {
	stgdOpType, stgdData, docExists := fetchStagedOpData(t, agent, key)

	assert.Equal(t, expOpType, stgdOpType)
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
	assert.ErrorIs(t, memdx.ErrSubDocPathNotFound, existsOp.Err)
}

func TestTransactionsInsert(t *testing.T) {
	ctx := context.Background()
	agent := createDefaultAgent(t)
	txns, txn := initTransactionAndAttempt(t, agent)

	docKey := []byte("txnInsertDoc")
	docValue := []byte(`{"name":"mike"}`)

	_, err := txn.Insert(ctx, transactionsx.TransactionInsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          docValue,
	})
	require.NoError(t, err)

	txn2, err := txns.BeginTransaction(nil)
	require.NoError(t, err)

	err = txn2.NewAttempt()
	require.NoError(t, err)

	_, err = txn2.Get(ctx, transactionsx.TransactionGetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	assert.Error(t, err)
	assert.ErrorIs(t, transactionsx.ErrDocNotFound, err)

	assertStagedDoc(t, agent, docKey, "insert", docValue, true)

	err = txn.Commit(ctx)
	require.NoError(t, err)

	assertDocNotStaged(t, agent, docKey)
}
