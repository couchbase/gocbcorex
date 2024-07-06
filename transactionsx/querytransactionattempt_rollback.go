package transactionsx

import (
	"context"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
)

func (t *QueryTransactionAttempt) Rollback(ctx context.Context) (*TransactionAttemptResult, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.canCommit = false

	_, _, err := nonStreamingQuery(ctx, t.txAgent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement:  "ROLLBACK",
			TxId:       t.queryTxId,
			OnBehalfOf: t.txOnBehalfOf,
		},
		Endpoint: t.queryEndpoint,
	})
	if err != nil {
		// TODO(brett19): This should have the result...
		return nil, &TransactionAttemptError{
			Cause:  err,
			Result: nil,
		}
	}

	return &TransactionAttemptResult{
		State:                 TransactionAttemptStateRolledBack,
		ID:                    t.queryTxId,
		AtrBucketName:         "",
		AtrScopeName:          "",
		AtrCollectionName:     "",
		AtrID:                 []byte(""),
		UnstagingComplete:     true,
		Expired:               false,
		PreExpiryAutoRollback: false,
	}, nil
}
