package cbqueryx_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestQueryDurabilityErrorCauses drives durable DML statements that fail, and
// verifies we decode the nested cause chain the query service propagates from
// the data service. It covers the two distinct structures we can reliably
// trigger today, which nest the terminal KV status under different keys:
//
//   - an unusably short kv timeout, which the data service rejects up front as
//     DURABILITY_INVALID_LEVEL (terminal nested under "error"), and
//   - a short-but-valid kv timeout, which cannot confirm the durable write in
//     time and yields SYNC_WRITE_AMBIGUOUS (terminal nested under "cause").
func TestQueryDurabilityErrorCauses(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()

	nodes := testutilsint.GetTestNodes(t)
	queryNode := nodes.SelectFirst(t, func(node *testutilsint.NodeTarget) bool {
		return node.QueryPort > 0
	})

	query := cbqueryx.Query{
		Logger:    testutils.MakeTestLogger(t),
		Transport: http.DefaultTransport,
		UserAgent: "durability-error-test",
		Endpoint:  queryNode.QueryEndpoint(),
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}

	// runUpsert issues a durable UPSERT with the given kv timeout, retrying with
	// fresh keys until it observes a failure (the ambiguity case sits in a
	// narrow timing window on an idle cluster), and returns the ServerError.
	runUpsert := func(t *testing.T, kvTimeout time.Duration) *cbqueryx.ServerError {
		t.Helper()

		const maxAttempts = 50
		var queryErr error
		for i := 0; i < maxAttempts && queryErr == nil; i++ {
			opts := &cbqueryx.QueryOptions{
				Statement: fmt.Sprintf(
					"UPSERT INTO `%s` (KEY, VALUE) VALUES (%q, {\"test\": \"value\"})",
					testutilsint.TestOpts.BucketName,
					"durability-err-"+uuid.NewString()[:8],
				),
				DurabilityLevel: cbqueryx.DurabilityLevelMajority,
				KvTimeout:       kvTimeout,
			}

			res, err := query.Query(ctx, opts)
			if err == nil {
				for res.HasMoreRows() {
					_, _ = res.ReadRow()
				}
				_, err = res.MetaData()
			}
			queryErr = err
		}
		require.Error(t, queryErr, "failed to provoke a durability error")

		t.Logf("error: %s", queryErr)

		var serverErr *cbqueryx.ServerError
		require.ErrorAs(t, queryErr, &serverErr)
		return serverErr
	}

	deepest := func(c *cbqueryx.QueryErrorCause) *cbqueryx.QueryErrorCause {
		for c != nil && c.Cause != nil {
			c = c.Cause
		}
		return c
	}

	t.Run("DurabilityInvalidLevel", func(t *testing.T) {
		// A 1ns timeout is too low to specify a durability timeout at all, so
		// the data service rejects it before attempting the write.
		serverErr := runUpsert(t, 1*time.Nanosecond)

		require.Equal(t, uint32(12009), serverErr.Code)
		require.NotNil(t, serverErr.Cause)
		require.Equal(t, uint32(5502), serverErr.Cause.Code)
		require.Equal(t, "DURABILITY_INVALID_LEVEL", deepest(serverErr.Cause).Message)
	})

	t.Run("SyncWriteAmbiguous", func(t *testing.T) {
		// A 1ms timeout is valid but too short to confirm the durable write.
		serverErr := runUpsert(t, 1*time.Millisecond)

		require.Equal(t, uint32(12009), serverErr.Code)
		require.NotNil(t, serverErr.Cause)
		require.Equal(t, uint32(5502), serverErr.Cause.Code)
		require.Equal(t, "SYNC_WRITE_AMBIGUOUS", deepest(serverErr.Cause).Message)
	})
}
