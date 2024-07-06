package transactionsx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
)

type TransactionQueryOptions struct {
	Agent      *gocbcorex.Agent
	OnBehalfOf *cbhttpx.OnBehalfOfInfo

	Args            []json.RawMessage
	ClientContextId string
	ScanConsistency cbqueryx.ScanConsistency
	PipelineBatch   uint32
	PipelineCap     uint32
	Profile         cbqueryx.ProfileMode
	QueryContext    string
	ReadOnly        bool
	ScanCap         uint32
	ScanWait        time.Duration
	Statement       string

	NamedArgs map[string]json.RawMessage
	Raw       map[string]json.RawMessage
}

type TransactionQueryResult struct {
	Rows []json.RawMessage
}

func (t *QueryTransactionAttempt) Query(ctx context.Context, opts TransactionQueryOptions) (*TransactionQueryResult, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	rows, _, err := nonStreamingQuery(ctx, opts.Agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Args:            opts.Args,
			ClientContextId: opts.ClientContextId,
			ScanConsistency: opts.ScanConsistency,
			PipelineBatch:   opts.PipelineBatch,
			PipelineCap:     opts.PipelineCap,
			Profile:         opts.Profile,
			QueryContext:    opts.QueryContext,
			ReadOnly:        opts.ReadOnly,
			ScanCap:         opts.ScanCap,
			ScanWait:        opts.ScanWait,
			Statement:       opts.Statement,

			NamedArgs: opts.NamedArgs,
			Raw:       opts.Raw,

			TxId:       t.queryTxId,
			OnBehalfOf: opts.OnBehalfOf,
		},
		Endpoint: t.queryEndpoint,
	})
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	return &TransactionQueryResult{
		Rows: rows,
	}, nil
}
