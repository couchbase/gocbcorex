package transactionsx

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/pkg/errors"
)

type QueryTransactionAttempt struct {
	expiryTime      time.Time
	durabilityLevel cbqueryx.DurabilityLevel
	txAgent         *gocbcorex.Agent
	txOnBehalfOf    *cbhttpx.OnBehalfOfInfo

	lock          sync.Mutex
	queryEndpoint string
	queryTxId     string
	canCommit     bool
	shouldRetry   bool
}

type QueryTransactionOptions struct {
	Agent      *gocbcorex.Agent
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func NewQueryTransactionAttempt(
	ctx context.Context,
	attempt *TransactionAttempt,
	opts QueryTransactionOptions,
) (*QueryTransactionAttempt, error) {
	durabilityLevel, err := durabilityLevelToQueryx(attempt.durabilityLevel)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	serAttempt, err := attempt.Serialize(ctx)
	if err != nil {
		return nil, err
	}

	// we need to mark the transaction as unusable after we do this.
	attempt.applyStateBits(transactionStateBitShouldNotCommit|transactionStateBitShouldNotRollback, TransactionErrorReasonSuccess)

	// calculate the time left from the serialized transaction
	timeLeft := time.Duration(serAttempt.State.TimeLeftMs) * time.Millisecond
	expiryTime := time.Now().Add(timeLeft)

	// canCommit is always true because its disallowed to serialize a transaction that cannot continue
	canCommit := true

	serBytes, err := json.Marshal(serAttempt)
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	_, endpoint, err := nonStreamingQuery(ctx, opts.Agent, &gocbcorex.QueryOptions{
		QueryOptions: cbqueryx.QueryOptions{
			Statement:       "BEGIN WORK",
			DurabilityLevel: durabilityLevel,
			TxTimeout:       timeLeft,
			TxData:          serBytes,
			OnBehalfOf:      opts.OnBehalfOf,
		},
	})
	if err != nil {
		// TODO(brett19): This should be an operation error.
		return nil, err
	}

	// Create a new QueryTransactionsAttempt
	return &QueryTransactionAttempt{
		expiryTime:      expiryTime,
		txAgent:         opts.Agent,
		txOnBehalfOf:    opts.OnBehalfOf,
		durabilityLevel: durabilityLevel,
		queryEndpoint:   endpoint,
		queryTxId:       serAttempt.ID.Attempt,
		canCommit:       canCommit,
		shouldRetry:     true,
	}, nil
}

func (t *QueryTransactionAttempt) HasExpired() bool {
	return !time.Now().Before(t.expiryTime)
}

func (t *QueryTransactionAttempt) CanCommit() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.canCommit
}

func (t *QueryTransactionAttempt) ShouldRetry() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.shouldRetry
}

func nonStreamingQuery(ctx context.Context, agent *gocbcorex.Agent, opts *gocbcorex.QueryOptions) ([]json.RawMessage, string, error) {
	argsBytes, _ := json.Marshal(opts.Args)

	log.Printf("QUERY - %s", opts.Statement)
	log.Printf("  ARGS: %s", argsBytes)
	log.Printf("  TXDATA: %s", opts.TxData)

	res, err := agent.Query(ctx, opts)
	if err != nil {
		return nil, "", err
	}

	var rows []json.RawMessage
	for res.HasMoreRows() {
		row, err := res.ReadRow()
		if err != nil {
			return nil, "", wrapError(err, "failed to read row from query")
		}

		rows = append(rows, row)
	}

	meta, _ := res.MetaData()
	endpoint := res.Endpoint()

	log.Printf("RESULT:")
	log.Printf("  OPTS: %+v\n", opts)
	log.Printf("  ROWS:\n")
	for _, row := range rows {
		log.Printf("    %s", row)
	}
	log.Printf("  META: %+v", meta)
	log.Printf("  ENDPOINT: %s", endpoint)

	return rows, endpoint, nil
}

func oneResultQuery(
	ctx context.Context,
	agent *gocbcorex.Agent,
	opts *gocbcorex.QueryOptions,
	resOut interface{},
) (string, error) {
	rows, endpoint, err := nonStreamingQuery(ctx, agent, opts)
	if err != nil {
		return "", err
	}

	if len(rows) != 1 {
		return "", errors.New("expected exactly one row")
	}

	err = json.Unmarshal(rows[0], resOut)
	if err != nil {
		return "", wrapError(err, "failed to unmarshal result")
	}

	return endpoint, nil
}
