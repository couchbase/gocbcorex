package transactionsx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

type TransactionQueryOptions struct {
	Agent   *gocbcorex.Agent
	OboUser string

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

	NamedArgs map[string]json.RawMessage
	Raw       map[string]interface{}
}

type TransactionQueryResult struct {
	Rows []json.RawMessage
}

func (t *TransactionAttempt) Query(ctx context.Context, opts TransactionQueryOptions) (*TransactionQueryResult, error) {
	result, errSt := t.query(ctx, opts, false)
	if errSt != nil {
		t.logger.Info("query failed", zap.Error(errSt.Err()))
		return nil, t.processOpStatus(ctx, errSt)
	}

	return result, nil
}

func (t *TransactionAttempt) PreparedQuery(ctx context.Context, opts TransactionQueryOptions) (*TransactionQueryResult, error) {
	result, errSt := t.query(ctx, opts, true)
	if errSt != nil {
		t.logger.Info("prepared query failed", zap.Error(errSt.Err()))
		return nil, t.processOpStatus(ctx, errSt)
	}

	return result, nil
}

func (t *TransactionAttempt) query(ctx context.Context, opts TransactionQueryOptions, isPrepared bool) (*TransactionQueryResult, *TransactionOperationStatus) {
	t.lock.Lock()

	if !t.isQueryMode {
		err := t.queryModeBegin(ctx)
		if err != nil {
			return nil, err
		}

		// query mode is now enabled
	}

	queryOpts := &cbqueryx.Options{
		TxId: t.id,

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
		NamedArgs:       opts.NamedArgs,
		Raw:             opts.NamedArgs,
	}

	var result cbqueryx.ResultStream
	var err error
	if !isPrepared {
		result, err = opts.Agent.Query(ctx, queryOpts)
	} else {
		result, err = opts.Agent.PreparedQuery(ctx, queryOpts)
	}
	if err != nil {
		classifyError(err)
	}

	var rows []json.RawMessage
	for result.HasMoreRows() {
		row, err := result.ReadRow()
		if err != nil {
			return nil, t.operationNotImplemented()
		}

		rows = append(rows, row)
	}

	// TODO(brett19): Actually implement Query...
	return &TransactionQueryResult{
		Rows: rows,
	}, nil
}
