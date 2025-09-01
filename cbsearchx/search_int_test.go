package cbsearchx_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func defaultSearch(t *testing.T) *cbsearchx.Search {
	nodes := testutilsint.GetTestNodes(t)
	searchNode := nodes.SelectFirst(t, func(node *testutilsint.NodeTarget) bool {
		return node.SearchPort > 0
	})

	return &cbsearchx.Search{
		Logger:    testutils.MakeTestLogger(t),
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  searchNode.SearchEndpoint(),
		Auth: &cbhttpx.BasicAuth{
			Username: testutilsint.TestOpts.Username,
			Password: testutilsint.TestOpts.Password,
		},
	}
}

func defaultOpts(indexName string) cbsearchx.UpsertIndexOptions {
	return cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
		OnBehalfOf: nil,
	}
}

func scopedOpts(indexName string) cbsearchx.UpsertIndexOptions {
	opts := defaultOpts(indexName)
	opts.BucketName = "someBucket"
	opts.ScopeName = "someScope"
	return opts
}

func TestUpsertIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	search := defaultSearch(t)
	require.NotNil(t, search)
	indexName := "upsert" + newSearchIndexName()
	scopedIndexName := newSearchIndexName()
	ctx := context.Background()

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		if testutilsint.SupportsFeature(testutilsint.TestFeatureScopedSearch) {
			err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
			require.NoError(t, err)
		}
	})

	t.Run("SuccessInsert", func(t *testing.T) {
		opts := defaultOpts(indexName)
		resp, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.UUID)
		if !testutilsint.IsOlderServerVersion(t, "7.6.0") {
			assert.Equal(t, indexName, resp.Name)
		}
	})

	t.Run("SuccessUpdate", func(t *testing.T) {
		index, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		opts := defaultOpts(indexName)
		opts.UUID = index.UUID
		resp, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.UUID)
		if !testutilsint.IsOlderServerVersion(t, "7.6.0") {
			assert.Equal(t, indexName, resp.Name)
		}
	})

	t.Run("UUIDMismatch", func(t *testing.T) {
		opts := defaultOpts(indexName)
		_, err := search.UpsertIndex(ctx, &opts)

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, cbsearchx.ErrIndexExists, sErr.Cause)
	})

	t.Run("MissingName", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Name = ""
		_, err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("MissingType", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Type = ""
		_, err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("MissingIndexSourceType", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.SourceType = ""
		_, err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("IndexSourceTypeIncorrect", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.SourceType = "magma"
		_, err := search.UpsertIndex(ctx, &opts)

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, cbsearchx.ErrSourceTypeIncorrect, sErr.Cause)
	})

	t.Run("IndexTypeInvalid", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Type = "notAType"
		_, err := search.UpsertIndex(ctx, &opts)

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, cbsearchx.ErrUnknownIndexType, sErr.Cause)
	})

	t.Run("IndexSourceNotFound", func(t *testing.T) {
		// 7.0.3 enterprise returns the same message for an incorrect source type as for a
		// source that cannot be found
		testutilsint.SkipIfOlderServerVersion(t, "7.2.2")
		opts := defaultOpts(indexName)
		opts.SourceName = "sourceNotFound"
		_, err := search.UpsertIndex(ctx, &opts)

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, cbsearchx.ErrSourceNotFound, sErr.Cause)
	})

	t.Run("SuccessSpecificScope", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		resp, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.UUID)
		assert.Equal(t,
			fmt.Sprintf("%s.%s.%s", opts.BucketName, opts.ScopeName, scopedIndexName),
			resp.Name)
	})
}

func TestGetIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	ctx := context.Background()

	search := defaultSearch(t)
	indexName := "get" + newSearchIndexName()
	scopedIndexName := newSearchIndexName()
	opts := defaultOpts(indexName)
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		if testutilsint.SupportsFeature(testutilsint.TestFeatureScopedSearch) {
			err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
			require.NoError(t, err)
		}
	})

	t.Run("Success", func(t *testing.T) {
		index, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
		require.NotNil(t, index)
	})

	t.Run("MissingIndexName", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{})
		require.Equal(t, errors.New("must specify index name when getting an index"), err)
	})

	t.Run("MissingBucketName", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName: indexName,
			ScopeName: "_default",
		})
		require.Equal(t, errors.New("must specify both or neither of scope and bucket names"), err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName: "indexNotFound",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("SuccessScopedIndex", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		_, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		index, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
		require.NotNil(t, index)
	})

	t.Run("ScopeNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		_, err := search.GetIndex(ctx, &cbsearchx.GetIndexOptions{
			IndexName:  indexName,
			BucketName: testutilsint.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}

func TestDeleteIndex(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	search := defaultSearch(t)

	ctx := context.Background()

	indexName := newSearchIndexName()
	scopedIndexName := "delete" + newSearchIndexName()
	opts := defaultOpts(indexName)
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})

		if testutilsint.SupportsFeature(testutilsint.TestFeatureScopedSearch) {
			err = search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
		}
	})

	t.Run("Success", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName: "not-an-index",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("IndexNameMissing", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{})
		require.Equal(t, errors.New("must specify index name when deleting an index"), err)
	})

	t.Run("IndexMissingScope", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName:  indexName,
			BucketName: testutilsint.TestOpts.BucketName,
		})
		require.Equal(t, errors.New("must specify both or neither of scope and bucket names"), err)
	})

	t.Run("SuccessScopedSearch", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts.BucketName = "some-bucket"
		opts.ScopeName = "some-scope"
		_, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
		err = search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			BucketName: "some-bucket",
			ScopeName:  "some-scope",
			IndexName:  indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexScopeNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		err := search.DeleteIndex(ctx, &cbsearchx.DeleteIndexOptions{
			IndexName:  indexName,
			BucketName: testutilsint.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}

func TestGetAllIndexes(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	search := defaultSearch(t)

	ctx := context.Background()

	indexName := "getAll" + newSearchIndexName()
	opts := cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	}
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		// if testutilsint.SupportsFeature(testutilsint.TestFeatureScopedSearch) {
		// 	err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
		// 		IndexName: scopedIndexName,
		// 	})
		// 	require.NoError(t, err)
		// }
	})

	t.Run("Success", func(t *testing.T) {
		indexes, err := search.GetAllIndexes(ctx, &cbsearchx.GetAllIndexesOptions{})
		require.NoError(t, err)

		var indexNames []string
		for _, index := range indexes {
			indexNames = append(indexNames, index.Name)
		}
		require.Contains(t, indexNames, indexName)
	})

	t.Run("ScopeNameMissing", func(t *testing.T) {
		_, err := search.GetAllIndexes(ctx, &cbsearchx.GetAllIndexesOptions{
			BucketName: testutilsint.TestOpts.BucketName,
		})
		require.Equal(t, errors.New("must specify both or neither of scope and bucket names"), err)
	})

	t.Run("SuccessScopedSearch", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		temp := opts
		temp.BucketName = "some-bucket"
		temp.ScopeName = "some-scope"
		_, err := search.UpsertIndex(ctx, &temp)
		require.NoError(t, err)
		indexes, err := search.GetAllIndexes(ctx, &cbsearchx.GetAllIndexesOptions{
			BucketName: "some-bucket",
			ScopeName:  "some-scope",
		})
		require.NoError(t, err)
		var names []string
		for _, index := range indexes {
			names = append(names, index.Name)
		}
		require.Contains(t, names, indexName)
	})

	t.Run("ScopeNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		indexes, err := search.GetAllIndexes(ctx, &cbsearchx.GetAllIndexesOptions{
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(indexes))
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		indexes, err := search.GetAllIndexes(ctx, &cbsearchx.GetAllIndexesOptions{
			BucketName: testutilsint.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(indexes))
	})
}

func TestPartitionControl(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	indexName := "partition" + newSearchIndexName()
	scopedIndexName := "partitions" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.FreezePlan(ctx, &cbsearchx.FreezePlanOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.UnfreezePlan(ctx, &cbsearchx.UnfreezePlanOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.FreezePlan(ctx, &cbsearchx.FreezePlanOptions{
			IndexName: "notAnIndex",
		})

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		_, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.FreezePlan(ctx, &cbsearchx.FreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.UnfreezePlan(ctx, &cbsearchx.UnfreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		err = search.FreezePlan(ctx, &cbsearchx.FreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, err, cbsearchx.ErrIndexNotFound)
	})
}

func TestIngestControl(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	indexName := "ingest" + newSearchIndexName()
	scopedIndexName := "ingest" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.PauseIngest(ctx, &cbsearchx.PauseIngestOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.ResumeIngest(ctx, &cbsearchx.ResumeIngestOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.PauseIngest(ctx, &cbsearchx.PauseIngestOptions{
			IndexName: "notAnIndex",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)

		err = search.ResumeIngest(ctx, &cbsearchx.ResumeIngestOptions{
			IndexName: "notAnIndex",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		_, err = search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.PauseIngest(ctx, &cbsearchx.PauseIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.ResumeIngest(ctx, &cbsearchx.ResumeIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		err = search.PauseIngest(ctx, &cbsearchx.PauseIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)

		err = search.ResumeIngest(ctx, &cbsearchx.ResumeIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}

func TestQueryControl(t *testing.T) {
	testutilsint.SkipIfShortTest(t)
	indexName := "query" + newSearchIndexName()
	scopedIndexName := "query" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.DisallowQuerying(ctx, &cbsearchx.DisallowQueryingOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.AllowQuerying(ctx, &cbsearchx.AllowQueryingOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.DisallowQuerying(ctx, &cbsearchx.DisallowQueryingOptions{
			IndexName: "notAnIndex",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)

		err = search.AllowQuerying(ctx, &cbsearchx.AllowQueryingOptions{
			IndexName: "notAnIndex",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		scopedName := newSearchIndexName()
		opts := scopedOpts(scopedName)
		_, err = search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.DisallowQuerying(ctx, &cbsearchx.DisallowQueryingOptions{
			IndexName:  scopedName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.AllowQuerying(ctx, &cbsearchx.AllowQueryingOptions{
			IndexName:  scopedName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		err = search.DisallowQuerying(ctx, &cbsearchx.DisallowQueryingOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)

		err = search.AllowQuerying(ctx, &cbsearchx.AllowQueryingOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}

func TestAnalyzeDocument(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	search := defaultSearch(t)

	indexName := "analyze" + newSearchIndexName()
	opts := cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	}
	opts.PlanParams = make(map[string]json.RawMessage)
	opts.PlanParams["indexPartitions"] = []byte("3")

	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		aOpts := cbsearchx.AnalyzeDocumentOptions{
			IndexName:  indexName,
			DocContent: []byte("true"),
		}

		// Analyze document fails if the index has not yet been partitioned,
		// so we need to wait for that to happen
		require.Eventually(t, func() bool {
			analysis, err := search.AnalyzeDocument(ctx, &aOpts)
			if errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned) || errors.Is(err, cbsearchx.ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			require.Equal(t, "ok", analysis.Status)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("MissingDocContent", func(t *testing.T) {
		aOpts := cbsearchx.AnalyzeDocumentOptions{
			IndexName: indexName,
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		require.Error(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		aOpts := cbsearchx.AnalyzeDocumentOptions{
			IndexName:  "MissingIndex",
			DocContent: []byte("true"),
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		opts := cbsearchx.UpsertIndexOptions{
			BucketName: "someBucket",
			ScopeName:  "someScope",
			Index: cbsearchx.Index{
				Name:       indexName,
				Type:       "fulltext-index",
				SourceType: "couchbase",
				SourceName: testutilsint.TestOpts.BucketName,
			},
		}
		opts.PlanParams = make(map[string]json.RawMessage)
		opts.PlanParams["indexPartitions"] = []byte("3")

		_, err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		// err = searchComponent.EnsureIndexCreated(ctx, &EnsureSearchIndexCreatedOptions{
		// 	IndexName: indexName,
		// })
		// require.NoError(t, err)

		aOpts := cbsearchx.AnalyzeDocumentOptions{
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
			IndexName:  indexName,
			DocContent: []byte("true"),
		}
		analysis, err := search.AnalyzeDocument(ctx, &aOpts)
		require.NoError(t, err)
		require.Equal(t, "ok", analysis.Status)
	})

	t.Run("ScopedBucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)
		aOpts := cbsearchx.AnalyzeDocumentOptions{
			BucketName: "bucketNotFound",
			ScopeName:  "someScope",
			IndexName:  indexName,
			DocContent: []byte("true"),
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}

func TestGetIndexedDocumentsCount(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	ctx := context.Background()

	search := defaultSearch(t)
	indexName := "docCount" + newSearchIndexName()
	opts := cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	}

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	_, err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		require.Eventually(t, func() bool {
			_, err := search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
				IndexName: indexName,
			})
			if errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned) || errors.Is(err, cbsearchx.ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		_, err := search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
			IndexName: "indexNotFound",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	scopedName := newSearchIndexName()
	scopedOpts := cbsearchx.UpsertIndexOptions{
		BucketName: "someBucket",
		ScopeName:  "someScope",
		Index: cbsearchx.Index{
			Name:       scopedName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutilsint.TestOpts.BucketName,
		},
	}

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)

		_, err := search.UpsertIndex(ctx, &scopedOpts)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err := search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
				IndexName: scopedName,
			})
			if errors.Is(err, cbsearchx.ErrNoIndexPartitionsPlanned) || errors.Is(err, cbsearchx.ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("ScopedIndexNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)

		_, err = search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
			BucketName: scopedOpts.BucketName,
			ScopeName:  scopedOpts.ScopeName,
			IndexName:  "indexNotFound",
		})
		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})

	t.Run("ScopedBucketNotFound", func(t *testing.T) {
		testutilsint.SkipIfUnsupportedFeature(t, testutilsint.TestFeatureScopedSearch)

		_, err = search.GetIndexedDocumentsCount(ctx, &cbsearchx.GetIndexedDocumentsCountOptions{
			BucketName: "bucketNotFound",
			ScopeName:  scopedOpts.ScopeName,
			IndexName:  scopedName,
		})

		var sErr *cbsearchx.SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, cbsearchx.ErrIndexNotFound)
	})
}
