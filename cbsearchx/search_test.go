package cbsearchx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func defaultSearch(t *testing.T) *Search {
	config, err := cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testutils.TestOpts.HTTPAddrs[0],
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}.GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{
		OnBehalfOf: nil,
	})
	if err != nil {
		return nil
	}

	var endpoint string
	for _, nodeExt := range config.NodesExt {
		if nodeExt.Services.Fts > 0 {
			endpoint = fmt.Sprintf("http://%s:%d", nodeExt.Hostname, nodeExt.Services.Fts)
			break
		}
	}

	if endpoint == "" {
		t.Skip("Skipping test, no search nodes")
		return nil
	}

	return &Search{
		Logger:    testutils.MakeTestLogger(t),
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  endpoint,
		Username:  testutils.TestOpts.Username,
		Password:  testutils.TestOpts.Password,
	}
}

func defaultOpts(indexName string) UpsertIndexOptions {
	return UpsertIndexOptions{
		Index: Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
		OnBehalfOf: nil,
	}
}

func scopedOpts(indexName string) UpsertIndexOptions {
	opts := defaultOpts(indexName)
	opts.BucketName = "someBucket"
	opts.ScopeName = "someScope"
	return opts
}

func TestUpsertIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)

	search := defaultSearch(t)
	require.NotNil(t, search)
	indexName := "upsert" + newSearchIndexName()
	scopedIndexName := newSearchIndexName()
	ctx := context.Background()

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		if testutils.SupportsFeature(testutils.TestFeatureScopedSearch) {
			err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
			require.NoError(t, err)
		}
	})

	t.Run("SuccessInsert", func(t *testing.T) {
		opts := defaultOpts(indexName)
		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
	})

	t.Run("SuccessUpdate", func(t *testing.T) {
		index, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		opts := defaultOpts(indexName)
		opts.UUID = index.UUID
		err = search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
	})

	t.Run("UUIDMismatch", func(t *testing.T) {
		opts := defaultOpts(indexName)
		err := search.UpsertIndex(ctx, &opts)

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, sErr.Cause, ErrIndexExists)
	})

	t.Run("MissingName", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Name = ""
		err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("MissingType", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Type = ""
		err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("MissingIndexSourceType", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.SourceType = ""
		err := search.UpsertIndex(ctx, &opts)
		require.NotNil(t, err)
	})

	t.Run("IndexSourceTypeIncorrect", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.SourceType = "magma"
		err := search.UpsertIndex(ctx, &opts)

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, sErr.Cause, ErrSourceTypeIncorrect)
	})

	t.Run("IndexTypeInvalid", func(t *testing.T) {
		opts := defaultOpts(indexName)
		opts.Type = "notAType"
		err := search.UpsertIndex(ctx, &opts)

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, sErr.Cause, ErrUnknownIndexType)
	})

	t.Run("IndexSourceNotFound", func(t *testing.T) {
		// 7.0.3 enterprise returns the same message for an incorrect source type as for a
		// source that cannot be found
		testutils.SkipIfOlderServerVersion(t, "7.2.2")
		opts := defaultOpts(indexName)
		opts.SourceName = "sourceNotFound"
		err := search.UpsertIndex(ctx, &opts)

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.Equal(t, ErrSourceNotFound, sErr.Cause)
	})

	t.Run("SuccessSpecificScope", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
	})
}

func TestGetIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)
	ctx := context.Background()

	search := defaultSearch(t)
	indexName := "get" + newSearchIndexName()
	scopedIndexName := newSearchIndexName()
	opts := defaultOpts(indexName)
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		if testutils.SupportsFeature(testutils.TestFeatureScopedSearch) {
			err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
			require.NoError(t, err)
		}
	})

	t.Run("Success", func(t *testing.T) {
		index, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
		require.NotNil(t, index)
	})

	t.Run("MissingIndexName", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &GetIndexOptions{})
		require.Equal(t, err, errors.New("must specify index name when getting an index"))
	})

	t.Run("MissingBucketName", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName: indexName,
			ScopeName: "_default",
		})
		require.Equal(t, errors.New("must specify both or neither of scope and bucket names"), err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		_, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName: "indexNotFound",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})

	t.Run("SuccessScopedIndex", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		index, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
		require.NotNil(t, index)
	})

	t.Run("ScopeNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		_, err := search.GetIndex(ctx, &GetIndexOptions{
			IndexName:  indexName,
			BucketName: testutils.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}

func TestDeleteIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)
	search := defaultSearch(t)

	ctx := context.Background()

	indexName := newSearchIndexName()
	scopedIndexName := "delete" + newSearchIndexName()
	opts := defaultOpts(indexName)
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})

		if testutils.SupportsFeature(testutils.TestFeatureScopedSearch) {
			err = search.DeleteIndex(context.Background(), &DeleteIndexOptions{
				IndexName: scopedIndexName,
			})
		}
	})

	t.Run("Success", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &DeleteIndexOptions{
			IndexName: "not-an-index",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})

	t.Run("IndexNameMissing", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &DeleteIndexOptions{})
		require.Equal(t, errors.New("must specify index name when deleting an index"), err)
	})

	t.Run("IndexMissingScope", func(t *testing.T) {
		err := search.DeleteIndex(ctx, &DeleteIndexOptions{
			IndexName:  indexName,
			BucketName: testutils.TestOpts.BucketName,
		})
		require.Equal(t, errors.New("must specify both or neither of scope and bucket names"), err)
	})

	t.Run("SuccessScopedSearch", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts.BucketName = "some-bucket"
		opts.ScopeName = "some-scope"
		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)
		err = search.DeleteIndex(ctx, &DeleteIndexOptions{
			BucketName: "some-bucket",
			ScopeName:  "some-scope",
			IndexName:  indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexScopeNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		err := search.DeleteIndex(ctx, &DeleteIndexOptions{
			IndexName:  indexName,
			BucketName: testutils.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}

func TestGetAllIndexes(t *testing.T) {
	testutils.SkipIfShortTest(t)
	search := defaultSearch(t)

	ctx := context.Background()

	indexName := "getAll" + newSearchIndexName()
	opts := UpsertIndexOptions{
		Index: Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	}
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		// if testutils.SupportsFeature(testutils.TestFeatureScopedSearch) {
		// 	err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
		// 		IndexName: scopedIndexName,
		// 	})
		// 	require.NoError(t, err)
		// }
	})

	t.Run("Success", func(t *testing.T) {
		indexes, err := search.GetAllIndexes(ctx, &GetAllIndexesOptions{})
		require.NoError(t, err)

		var indexNames []string
		for _, index := range indexes {
			indexNames = append(indexNames, index.Name)
		}
		require.Contains(t, indexNames, indexName)
	})

	t.Run("ScopeNameMissing", func(t *testing.T) {
		_, err := search.GetAllIndexes(ctx, &GetAllIndexesOptions{
			BucketName: testutils.TestOpts.BucketName,
		})
		require.Equal(t, err, errors.New("must specify both or neither of scope and bucket names"))
	})

	t.Run("SuccessScopedSearch", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		temp := opts
		temp.BucketName = "some-bucket"
		temp.ScopeName = "some-scope"
		err := search.UpsertIndex(ctx, &temp)
		require.NoError(t, err)
		indexes, err := search.GetAllIndexes(ctx, &GetAllIndexesOptions{
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
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		indexes, err := search.GetAllIndexes(ctx, &GetAllIndexesOptions{
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(indexes))
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		indexes, err := search.GetAllIndexes(ctx, &GetAllIndexesOptions{
			BucketName: testutils.TestOpts.BucketName,
			ScopeName:  "scopeNotFound",
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(indexes))
	})
}

func TestPartitionControl(t *testing.T) {
	testutils.SkipIfShortTest(t)
	indexName := "partition" + newSearchIndexName()
	scopedIndexName := "partitions" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.FreezePlan(ctx, &FreezePlanOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.UnfreezePlan(ctx, &UnfreezePlanOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.FreezePlan(ctx, &FreezePlanOptions{
			IndexName: "notAnIndex",
		})

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, ErrIndexNotFound, sErr.Cause)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.FreezePlan(ctx, &FreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.UnfreezePlan(ctx, &UnfreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		err = search.FreezePlan(ctx, &FreezePlanOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, err, ErrIndexNotFound)
	})
}

func TestIngestControl(t *testing.T) {
	testutils.SkipIfShortTest(t)
	indexName := "ingest" + newSearchIndexName()
	scopedIndexName := "ingest" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.PauseIngest(ctx, &PauseIngestOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.ResumeIngest(ctx, &ResumeIngestOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.PauseIngest(ctx, &PauseIngestOptions{
			IndexName: "notAnIndex",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, ErrIndexNotFound, sErr.Cause)

		err = search.ResumeIngest(ctx, &ResumeIngestOptions{
			IndexName: "notAnIndex",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, ErrIndexNotFound, sErr.Cause)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts := scopedOpts(scopedIndexName)
		err = search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.PauseIngest(ctx, &PauseIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.ResumeIngest(ctx, &ResumeIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		err = search.PauseIngest(ctx, &PauseIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)

		err = search.ResumeIngest(ctx, &ResumeIngestOptions{
			IndexName:  scopedIndexName,
			BucketName: "bucketNotFound",
			ScopeName:  "_default",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}

func TestQueryControl(t *testing.T) {
	testutils.SkipIfShortTest(t)
	indexName := "query" + newSearchIndexName()
	scopedIndexName := "query" + newSearchIndexName()

	search := defaultSearch(t)
	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	opts := defaultOpts(indexName)
	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		err = search.DisallowQuerying(ctx, &DisallowQueryingOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)

		err = search.AllowQuerying(ctx, &AllowQueryingOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		err = search.DisallowQuerying(ctx, &DisallowQueryingOptions{
			IndexName: "notAnIndex",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, ErrIndexNotFound, sErr.Cause)

		err = search.AllowQuerying(ctx, &AllowQueryingOptions{
			IndexName: "notAnIndex",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, ErrIndexNotFound, sErr.Cause)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		scopedName := newSearchIndexName()
		opts := scopedOpts(scopedName)
		err = search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		err = search.DisallowQuerying(ctx, &DisallowQueryingOptions{
			IndexName:  scopedName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)

		err = search.AllowQuerying(ctx, &AllowQueryingOptions{
			IndexName:  scopedName,
			BucketName: opts.BucketName,
			ScopeName:  opts.ScopeName,
		})
		require.NoError(t, err)
	})

	t.Run("BucketNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		err = search.DisallowQuerying(ctx, &DisallowQueryingOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)

		err = search.AllowQuerying(ctx, &AllowQueryingOptions{
			IndexName:  scopedIndexName,
			BucketName: "notABucket",
			ScopeName:  "_default",
		})
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}

func TestAnalyzeDocument(t *testing.T) {
	testutils.SkipIfShortTest(t)

	search := defaultSearch(t)

	indexName := "analyze" + newSearchIndexName()
	opts := UpsertIndexOptions{
		Index: Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	}
	opts.Index.PlanParams = make(map[string]json.RawMessage)
	opts.Index.PlanParams["indexPartitions"] = []byte("3")

	ctx := context.Background()
	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		aOpts := AnalyzeDocumentOptions{
			IndexName:  indexName,
			DocContent: []byte("true"),
		}

		// Analyze document fails if the index has not yet been partitioned,
		// so we need to wait for that to happen
		require.Eventually(t, func() bool {
			analysis, err := search.AnalyzeDocument(ctx, &aOpts)
			if errors.Is(err, ErrNoIndexPartitionsPlanned) || errors.Is(err, ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			require.Equal(t, "ok", analysis.Status)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("MissingDocContent", func(t *testing.T) {
		aOpts := AnalyzeDocumentOptions{
			IndexName: indexName,
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		require.Error(t, err)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		aOpts := AnalyzeDocumentOptions{
			IndexName:  "MissingIndex",
			DocContent: []byte("true"),
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		opts := UpsertIndexOptions{
			BucketName: "someBucket",
			ScopeName:  "someScope",
			Index: Index{
				Name:       indexName,
				Type:       "fulltext-index",
				SourceType: "couchbase",
				SourceName: testutils.TestOpts.BucketName,
			},
		}
		opts.Index.PlanParams = make(map[string]json.RawMessage)
		opts.Index.PlanParams["indexPartitions"] = []byte("3")

		err := search.UpsertIndex(ctx, &opts)
		require.NoError(t, err)

		// err = searchComponent.EnsureIndexCreated(ctx, &EnsureSearchIndexCreatedOptions{
		// 	IndexName: indexName,
		// })
		// require.NoError(t, err)

		aOpts := AnalyzeDocumentOptions{
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
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)
		aOpts := AnalyzeDocumentOptions{
			BucketName: "bucketNotFound",
			ScopeName:  "someScope",
			IndexName:  indexName,
			DocContent: []byte("true"),
		}
		_, err := search.AnalyzeDocument(ctx, &aOpts)
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}

func TestGetIndexedDocumentsCount(t *testing.T) {
	testutils.SkipIfShortTest(t)

	ctx := context.Background()

	search := defaultSearch(t)
	indexName := "docCount" + newSearchIndexName()
	opts := UpsertIndexOptions{
		Index: Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	}

	t.Cleanup(func() {
		err := search.DeleteIndex(context.Background(), &DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err := search.UpsertIndex(ctx, &opts)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		require.Eventually(t, func() bool {
			_, err := search.GetIndexedDocumentsCount(ctx, &GetIndexedDocumentsCountOptions{
				IndexName: indexName,
			})
			if errors.Is(err, ErrNoIndexPartitionsPlanned) || errors.Is(err, ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("IndexNotFound", func(t *testing.T) {
		_, err := search.GetIndexedDocumentsCount(ctx, &GetIndexedDocumentsCountOptions{
			IndexName: "indexNotFound",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})

	scopedName := newSearchIndexName()
	scopedOpts := UpsertIndexOptions{
		BucketName: "someBucket",
		ScopeName:  "someScope",
		Index: Index{
			Name:       scopedName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	}

	t.Run("ScopedSuccess", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)

		err := search.UpsertIndex(ctx, &scopedOpts)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err := search.GetIndexedDocumentsCount(ctx, &GetIndexedDocumentsCountOptions{
				IndexName: scopedName,
			})
			if errors.Is(err, ErrNoIndexPartitionsPlanned) || errors.Is(err, ErrNoIndexPartitionsFound) {
				return false
			}
			require.NoError(t, err)
			return true
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("ScopedIndexNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)

		_, err = search.GetIndexedDocumentsCount(ctx, &GetIndexedDocumentsCountOptions{
			BucketName: scopedOpts.BucketName,
			ScopeName:  scopedOpts.ScopeName,
			IndexName:  "indexNotFound",
		})
		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})

	t.Run("ScopedBucketNotFound", func(t *testing.T) {
		testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)

		_, err = search.GetIndexedDocumentsCount(ctx, &GetIndexedDocumentsCountOptions{
			BucketName: "bucketNotFound",
			ScopeName:  scopedOpts.ScopeName,
			IndexName:  scopedName,
		})

		var sErr SearchError
		require.ErrorAs(t, err, &sErr)
		require.ErrorIs(t, sErr.Cause, ErrIndexNotFound)
	})
}
