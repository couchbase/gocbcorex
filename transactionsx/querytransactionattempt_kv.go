package transactionsx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
)

func (t *QueryTransactionAttempt) genJsonKeyspaceName(bucketName, scopeName, collectionName string) []byte {
	keyspaceName := fmt.Sprintf("default:`%s`.`%s`.`%s`", bucketName, scopeName, collectionName)
	keyspaceBytes, _ := json.Marshal(keyspaceName)
	return keyspaceBytes
}

func (t *QueryTransactionAttempt) genJsonDocId(docId []byte) []byte {
	docIdBytes, _ := json.Marshal(string(docId))
	return docIdBytes
}

func (t *QueryTransactionAttempt) genKvTxData() json.RawMessage {
	return json.RawMessage(`{"kv":true}`)
}

func (t *QueryTransactionAttempt) genKvTxDataForDoc(cas uint64, txnMeta *MutableItemMeta) json.RawMessage {
	txData := make(map[string]interface{})
	txData["kv"] = true
	txData["scas"] = casToScas(cas)
	if txnMeta != nil {
		txData["txnMeta"] = txnMeta
	}

	txDataBytes, _ := json.Marshal(txData)
	return txDataBytes
}

func (t *QueryTransactionAttempt) Get(ctx context.Context, opts GetOptions) (*GetResult, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var result struct {
		Scas    string          `json:"scas"`
		Doc     json.RawMessage `json:"doc"`
		TxnMeta json.RawMessage `json:"txnMeta,omitempty"`
	}

	_, err := oneResultQuery(ctx, opts.Agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement: "EXECUTE __get",
			TxId:      t.queryTxId,
			TxData:    t.genKvTxData(),
			Args: []json.RawMessage{
				t.genJsonKeyspaceName(opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName),
				t.genJsonDocId(opts.Key)},
			OnBehalfOf: &cbhttpx.OnBehalfOfInfo{
				Username: opts.OboUser,
			},
		},
		Endpoint: t.queryEndpoint,
	}, &result)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	cas, err := scasToCas(result.Scas)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, wrapError(err, "failed to parse document cas")
	}

	var txnMeta *MutableItemMeta
	if result.TxnMeta != nil {
		err = json.Unmarshal(result.TxnMeta, &txnMeta)
		if err != nil {
			// TODO(brett19): This should be an operation error.
			return nil, wrapError(err, "failed to parse txn meta")
		}
	}

	return &GetResult{
		agent:          opts.Agent,
		oboUser:        opts.OboUser,
		scopeName:      opts.ScopeName,
		collectionName: opts.CollectionName,
		key:            opts.Key,

		Meta:  txnMeta,
		Value: result.Doc,
		Cas:   cas,
	}, nil
}

func (t *QueryTransactionAttempt) Replace(ctx context.Context, opts ReplaceOptions) (*GetResult, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var result struct {
		Scas string `json:"scas"`
	}

	_, err := oneResultQuery(ctx, opts.Document.agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement: "EXECUTE __update",
			TxId:      t.queryTxId,
			TxData:    t.genKvTxDataForDoc(opts.Document.Cas, opts.Document.Meta),
			Args: []json.RawMessage{
				t.genJsonKeyspaceName(opts.Document.agent.BucketName(),
					opts.Document.scopeName,
					opts.Document.collectionName),
				t.genJsonDocId(opts.Document.key),
				opts.Value,
				json.RawMessage("{}")},
			OnBehalfOf: &cbhttpx.OnBehalfOfInfo{
				Username: opts.Document.oboUser,
			},
		},
		Endpoint: t.queryEndpoint,
	}, &result)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	cas, err := scasToCas(result.Scas)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, wrapError(err, "failed to parse document cas")
	}

	return &GetResult{
		agent:          opts.Document.agent,
		oboUser:        opts.Document.oboUser,
		scopeName:      opts.Document.scopeName,
		collectionName: opts.Document.collectionName,
		key:            opts.Document.key,

		Meta:  nil,
		Value: opts.Value,
		Cas:   cas,
	}, nil
}

func (t *QueryTransactionAttempt) Insert(ctx context.Context, opts InsertOptions) (*GetResult, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var result struct {
		Scas string `json:"scas"`
	}

	_, err := oneResultQuery(ctx, opts.Agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement: "EXECUTE __insert",
			TxId:      t.queryTxId,
			TxData:    t.genKvTxData(),
			Args: []json.RawMessage{
				t.genJsonKeyspaceName(opts.Agent.BucketName(), opts.ScopeName, opts.CollectionName),
				t.genJsonDocId(opts.Key),
				opts.Value,
				json.RawMessage("{}")},
			OnBehalfOf: &cbhttpx.OnBehalfOfInfo{
				Username: opts.OboUser,
			},
		},
		Endpoint: t.queryEndpoint,
	}, &result)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	cas, err := scasToCas(result.Scas)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, wrapError(err, "failed to parse document cas")
	}

	return &GetResult{
		agent:          opts.Agent,
		oboUser:        opts.OboUser,
		scopeName:      opts.ScopeName,
		collectionName: opts.CollectionName,
		key:            opts.Key,

		Meta:  nil,
		Value: opts.Value,
		Cas:   cas,
	}, nil
}

func (t *QueryTransactionAttempt) Remove(ctx context.Context, opts RemoveOptions) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, _, err := nonStreamingQuery(ctx, opts.Document.agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement: "EXECUTE __delete`",
			TxId:      t.queryTxId,
			TxData:    t.genKvTxDataForDoc(opts.Document.Cas, opts.Document.Meta),
			Args: []json.RawMessage{
				t.genJsonKeyspaceName(opts.Document.agent.BucketName(),
					opts.Document.scopeName,
					opts.Document.collectionName),
				t.genJsonDocId(opts.Document.key),
				json.RawMessage("{}")},
			OnBehalfOf: &cbhttpx.OnBehalfOfInfo{
				Username: opts.Document.oboUser,
			},
		},
		Endpoint: t.queryEndpoint,
	})
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return err
	}

	return nil
}
