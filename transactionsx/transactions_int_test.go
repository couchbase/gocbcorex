package transactionsx_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/transactionsx"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var TEST_VALUE_INITIAL = []byte(`{"name":"initial-value"}`)
var TEST_VALUE = []byte(`{"name":"updated"}`)

func helperSetupBasicTest(
	t *testing.T,
) (
	context.Context,
	*gocbcorex.Agent,
	*transactionsx.TransactionsManager,
	*transactionsx.Transaction,
	*transactionsx.Transaction,
	[]byte,
) {
	ctx := context.Background()

	agent := createDefaultAgent(t)

	docKey := []byte(fmt.Sprintf("%s-%s", uuid.NewString(), t.Name()))

	txnMgr, txn1 := initTransactionAndAttempt(t, agent)
	_, txn2 := initTransactionAndAttempt(t, agent)

	return ctx, agent, txnMgr, txn1, txn2, docKey
}

func TestTransactionsInsertTxn1GetTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := txn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocMissing(t, agent, docKey)

	_, err = txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, transactionsx.ErrDocNotFound)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsInsertTxn1InsertTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := txn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocMissing(t, agent, docKey)

	_, err = txn2.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE,
	})
	assert.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsReplaceTxn1GetTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes,
		Value:    TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)
	assert.Equal(t, TEST_VALUE_INITIAL, getRes2.Value)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsReplaceTxn1InsertTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes,
		Value:    TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	_, err = txn2.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsReplaceTxn1ReplaceTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes,
		Value:    TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn2.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes2,
		Value:    TEST_VALUE,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsReplaceTxn1RemoveTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes,
		Value:    TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn2.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes2,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsRemoveTxn1GetTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, nil)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)
	assert.Equal(t, TEST_VALUE_INITIAL, getRes2.Value)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)
}

func TestTransactionsRemoveTxn1InsertTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, nil)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	_, err = txn2.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)
}

func TestTransactionsRemoveTxn1ReplaceTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, nil)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn2.Replace(ctx, transactionsx.ReplaceOptions{
		Document: getRes2,
		Value:    TEST_VALUE,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)
}

func TestTransactionsRemoveTxn1RemoveTxn2(t *testing.T) {
	ctx, agent, _, txn1, txn2, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	_, err := agent.Add(ctx, &gocbcorex.AddOptions{
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)

	getRes, err := txn1.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn1.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, nil)
	assertDocValue(t, agent, docKey, TEST_VALUE_INITIAL)

	getRes2, err := txn2.Get(ctx, transactionsx.GetOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
	})
	require.NoError(t, err)

	_, err = txn2.Remove(ctx, transactionsx.RemoveOptions{
		Document: getRes2,
	})
	require.Error(t, err)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)
}

func TestTransactionsInsertReplace(t *testing.T) {
	ctx, agent, _, txn1, _, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	insertRes, err := txn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE_INITIAL)
	assertDocMissing(t, agent, docKey)

	_, err = txn1.Replace(ctx, transactionsx.ReplaceOptions{
		Document: insertRes,
		Value:    TEST_VALUE,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE)
	assertDocMissing(t, agent, docKey)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocValue(t, agent, docKey, TEST_VALUE)
}

func TestTransactionsInsertRemove(t *testing.T) {
	ctx, agent, _, txn1, _, docKey := helperSetupBasicTest(t)
	defer func() { _ = agent.Close() }()

	insertRes, err := txn1.Insert(ctx, transactionsx.InsertOptions{
		Agent:          agent,
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            docKey,
		Value:          TEST_VALUE_INITIAL,
	})
	require.NoError(t, err)
	assertStagedDoc(t, agent, docKey, TEST_VALUE_INITIAL)
	assertDocMissing(t, agent, docKey)

	_, err = txn1.Remove(ctx, transactionsx.RemoveOptions{
		Document: insertRes,
	})
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)

	_, err = txn1.Commit(ctx)
	require.NoError(t, err)
	assertDocNotStaged(t, agent, docKey)
	assertDocMissing(t, agent, docKey)
}
