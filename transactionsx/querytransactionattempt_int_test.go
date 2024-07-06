package transactionsx_test

import (
	"log"
	"testing"

	"github.com/couchbase/gocbcorex/transactionsx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryTransactionAttemptSimple(t *testing.T) {
	ctx, agent, _, txn1, _, docKey := helperSetupBasicTest(t)
	defer agent.Close()

	_, err := txn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE_INITIAL)
	assertDocMissing(t, agent, docKey)

	txn1Atmp := txn1.Attempt()

	qtxn1, err := transactionsx.NewQueryTransactionAttempt(ctx, txn1Atmp, transactionsx.QueryTransactionOptions{
		Agent: agent,
	})
	require.NoError(t, err)

	getRes, err := qtxn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	assert.NoError(t, err)
	log.Printf("GET Result: %+v", getRes)

	repRes, err := qtxn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes,
		Value:    TEST_VALUE,
	})
	assert.NoError(t, err)
	log.Printf("REPLACE Result: %+v", repRes)

	_, err = qtxn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestQueryTransactionAttemptNoPreOps(t *testing.T) {
	ctx, agent, _, txn1, _, docKey := helperSetupBasicTest(t)
	defer agent.Close()

	txn1Atmp := txn1.Attempt()

	qtxn1, err := transactionsx.NewQueryTransactionAttempt(ctx, txn1Atmp, transactionsx.QueryTransactionOptions{
		Agent: agent,
	})
	require.NoError(t, err)

	getRes, err := qtxn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE,
	})
	assert.NoError(t, err)
	log.Printf("INSERT Result: %+v", getRes)

	_, err = qtxn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}
