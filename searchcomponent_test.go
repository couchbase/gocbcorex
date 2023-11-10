package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/testutils"

	"github.com/google/uuid"
)

func newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func TestUpsertGetDeleteSearchIndex(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	defer agent.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newSearchIndexName()

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	index, err := agent.GetSearchIndex(ctx, &cbsearchx.GetIndexOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	assert.Equal(t, indexName, index.Name)
	assert.Equal(t, "fulltext-index", index.Type)

	indexes, err := agent.GetAllSearchIndexes(ctx, &cbsearchx.GetAllIndexesOptions{})
	require.NoError(t, err)

	var found bool
	for _, i := range indexes {
		if i.Name == indexName {
			found = true
			break
		}
	}
	assert.True(t, found, "Did not find expected index in GetAllIndexes")

	err = agent.DeleteSearchIndex(ctx, &cbsearchx.DeleteIndexOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)
}

func TestSearchIndexesIngestControl(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newSearchIndexName()

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err = agent.PauseSearchIndexIngest(ctx, &cbsearchx.PauseIngestOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	err = agent.ResumeSearchIndexIngest(ctx, &cbsearchx.ResumeIngestOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)
}

func TestSearchIndexesQueryControl(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newSearchIndexName()

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err = agent.DisallowSearchIndexQuerying(ctx, &cbsearchx.DisallowQueryingOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	err = agent.AllowSearchIndexQuerying(ctx, &cbsearchx.AllowQueryingOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)
}

func TestSearchIndexesPartitionControl(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newSearchIndexName()

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err = agent.FreezeSearchIndexPlan(ctx, &cbsearchx.FreezePlanOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)

	err = agent.UnfreezeSearchIndexPlan(ctx, &cbsearchx.UnfreezePlanOptions{
		IndexName: indexName,
	})
	require.NoError(t, err)
}

func TestSearchUUIDMismatch(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newSearchIndexName()

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: indexName,
		})
		require.NoError(t, err)
	})

	err = agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: "couchbase",
			SourceName: testutils.TestOpts.BucketName,
			UUID:       "123545",
		},
	})
	assert.ErrorIs(t, err, cbsearchx.ErrIndexExists)
}
