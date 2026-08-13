package cbqueryx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requireBucketNotFound(t *testing.T, err error, bucketName string) {
	t.Helper()

	require.ErrorIs(t, err, ErrBucketNotFound)

	var resourceErr *ResourceError
	require.ErrorAs(t, err, &resourceErr)
	require.Equal(t, bucketName, resourceErr.BucketName)
	require.Empty(t, resourceErr.ScopeName)
	require.Empty(t, resourceErr.CollectionName)
}

func requireScopeNotFound(t *testing.T, err error, bucketName, scopeName string) {
	t.Helper()

	require.ErrorIs(t, err, ErrScopeNotFound)

	var resourceErr *ResourceError
	require.ErrorAs(t, err, &resourceErr)
	require.Equal(t, bucketName, resourceErr.BucketName)
	require.Equal(t, scopeName, resourceErr.ScopeName)
	require.Empty(t, resourceErr.CollectionName)
}

func requireCollectionNotFound(t *testing.T, err error, bucketName, scopeName, collectionName string) {
	t.Helper()

	require.ErrorIs(t, err, ErrCollectionNotFound)

	var resourceErr *ResourceError
	require.ErrorAs(t, err, &resourceErr)
	require.Equal(t, bucketName, resourceErr.BucketName)
	require.Equal(t, scopeName, resourceErr.ScopeName)
	require.Equal(t, collectionName, resourceErr.CollectionName)
}

func TestErrorParsing(t *testing.T) {
	t.Run("BucketNotFound", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			errJson := &queryErrorJson{
				Code: 12003,
				Msg:  "Keyspace not found in CB datastore: default:defaultx (near line 1, column 15) - cause: No bucket named defaultx",
			}

			err := parseError(errJson)
			requireBucketNotFound(t, err, "defaultx")
		})

		t.Run("BadURL", func(t *testing.T) {
			errJson := &queryErrorJson{
				Code: 12003,
				Msg:  "Keyspace not found in CB datastore: default:default - cause: Invalid URL (/pools/default/buckets/default?bucket_uuid=8b1f682c46869372f7b94144a7f7c065) response: empty vBucketMap",
			}

			err := parseError(errJson)
			requireBucketNotFound(t, err, "default")
		})

		t.Run("WithDots", func(t *testing.T) {
			errJson := &queryErrorJson{
				Code: 12003,
				Msg:  "Keyspace not found in CB datastore: default:default.x (near line 1, column 15) - cause: No bucket named default.x",
			}

			err := parseError(errJson)
			requireBucketNotFound(t, err, "default.x")
		})
	})

	t.Run("ScopeNotFound", func(t *testing.T) {
		errJson := &queryErrorJson{
			Code: 12021,
			Msg:  "Scope not found in CB datastore default:default.test (near line 1, column 15)",
		}

		err := parseError(errJson)
		requireScopeNotFound(t, err, "default", "test")
	})

	t.Run("CollectionNotFound", func(t *testing.T) {
		errJson := &queryErrorJson{
			Code: 12003,
			Msg:  "Keyspace not found in CB datastore: default:default._default.test (near line 1, column 15)",
		}

		err := parseError(errJson)
		requireCollectionNotFound(t, err, "default", "_default", "test")
	})
}

// deepestCause returns the terminal node of a cause chain.
func deepestCause(c *QueryErrorCause) *QueryErrorCause {
	for c != nil && c.Cause != nil {
		c = c.Cause
	}
	return c
}

// TestParseErrorCause verifies that the two distinct DML cause structures the
// query service emits (which nest the terminal KV status under different keys)
// both normalize into the same QueryErrorCause chain, with the KV status text
// reachable as the terminal node's Message.
func TestParseErrorCause(t *testing.T) {
	t.Run("SyncWriteAmbiguous", func(t *testing.T) {
		// Terminal KV status is a bare string nested under "cause".
		errJson := &queryErrorJson{
			Code:   12009,
			Msg:    "DML Error, possible causes include concurrent modification.",
			Reason: []byte(`{"code":5502,"key":"datastore.couchbase.bucket.action","message":"Unable to complete action after 1 attempts","cause":{"attempts":1,"cause":"SYNC_WRITE_AMBIGUOUS"}}`),
		}

		serverErr := parseError(errJson)

		require.NotNil(t, serverErr.Cause)
		require.Equal(t, uint32(5502), serverErr.Cause.Code)
		require.Equal(t, "datastore.couchbase.bucket.action", serverErr.Cause.Key)
		require.Equal(t, "SYNC_WRITE_AMBIGUOUS", deepestCause(serverErr.Cause).Message)
	})

	t.Run("DurabilityInvalidLevel", func(t *testing.T) {
		// Terminal KV status is a bare string nested under "error".
		errJson := &queryErrorJson{
			Code:   12009,
			Msg:    "DML Error, possible causes include concurrent modification.",
			Reason: []byte(`{"code":5502,"key":"datastore.couchbase.bucket.action","message":"Unable to complete action after 1 attempts","cause":{"attempts":1,"context":"Timeout(): Cannot specify bucket default timeout","error":"DURABILITY_INVALID_LEVEL"}}`),
		}

		serverErr := parseError(errJson)

		require.NotNil(t, serverErr.Cause)
		require.Equal(t, uint32(5502), serverErr.Cause.Code)
		require.Equal(t, "DURABILITY_INVALID_LEVEL", deepestCause(serverErr.Cause).Message)
	})

	t.Run("TransactionCauseKey", func(t *testing.T) {
		// Transaction errors deliver the cause under "cause" rather than
		// "reason"; both must decode identically.
		errJson := &queryErrorJson{
			Code:  17014,
			Msg:   "transaction error",
			Cause: []byte(`{"code":12033,"key":"datastore.couchbase.CAS_mismatch","message":"CAS mismatch"}`),
		}

		serverErr := parseError(errJson)

		require.NotNil(t, serverErr.Cause)
		require.Equal(t, uint32(12033), serverErr.Cause.Code)
		require.Equal(t, "CAS mismatch", serverErr.Cause.Message)
	})

	t.Run("NoCause", func(t *testing.T) {
		errJson := &queryErrorJson{
			Code: 3000,
			Msg:  "syntax error",
		}

		serverErr := parseError(errJson)

		require.Nil(t, serverErr.Cause)
	})
}
