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
