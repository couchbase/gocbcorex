package transactionsx_test

import (
	"context"
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
		DurabilityLevel: transactionsx.DurabilityLevelNone,
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
) []byte {
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
	require.NoError(t, err)

	typeOp := result.Ops[0]
	require.NoError(t, typeOp.Err)

	if string(typeOp.Value) == `"remove"` {
		return nil
	}

	stgdOp := result.Ops[1]
	require.NoError(t, stgdOp.Err)

	stgdData := stgdOp.Value

	// double-check the staged data isn't nil, since that would
	// break our checking for removes
	assert.NotNil(t, stgdData)

	return stgdData
}

func assertStagedDoc(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
	expStgdData []byte,
) {
	stgdData := fetchStagedOpData(t, agent, key)
	if expStgdData != nil {
		assert.Equal(t, expStgdData, stgdData)
	} else {
		assert.Nil(t, stgdData)
	}
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

func assertDocValue(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
	value []byte,
) {
	ctx := context.Background()

	getRes, err := agent.Get(ctx, &gocbcorex.GetOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            key,
	})
	require.NoError(t, err)
	assert.Equal(t, value, getRes.Value)
}

func assertDocMissing(
	t *testing.T,
	agent *gocbcorex.Agent,
	key []byte,
) {
	ctx := context.Background()

	_, err := agent.Get(ctx, &gocbcorex.GetOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            key,
	})
	assert.ErrorIs(t, err, memdx.ErrDocNotFound)
}
