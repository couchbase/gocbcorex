package gocbcorex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/gocbcorex/cbsearchx"

	"github.com/couchbase/gocbcorex/testutils"
)

func TestSearchBasic(t *testing.T) {
	testutils.SkipIfShortTest(t)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	helper := &searchTestHelper{
		TestName: "testSearchQuery",
		Agent:    agent,
		QueryFn:  agent.Search,
		T:        t,
	}

	if t.Run("Setup", helper.testSetupSearch) {

		t.Run("Basic", helper.testSearchBasic)

		t.Run("Cleanup", helper.testCleanupSearch)
	}
}

func TestSearchCollections(t *testing.T) {
	testutils.SkipIfShortTest(t)
	testutils.SkipIfUnsupportedFeature(t, testutils.TestFeatureScopedSearch)

	agent := CreateDefaultAgent(t)
	t.Cleanup(func() {
		err := agent.Close()
		require.NoError(t, err)
	})

	scopeName := "scope" + uuid.NewString()[:6]
	collectionName := "collection" + uuid.NewString()[:6]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	CreateAndEnsureScope(ctx, t, agent, testutils.TestOpts.BucketName, scopeName)
	CreateAndEnsureCollection(ctx, t, agent, testutils.TestOpts.BucketName, scopeName, collectionName)

	helper := &searchTestHelper{
		TestName:       "testSearchQuery" + uuid.NewString()[:6],
		Agent:          agent,
		QueryFn:        agent.Search,
		T:              t,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}

	if t.Run("Setup", helper.testSetupSearch) {

		t.Run("Basic", helper.testSearchBasic)

		t.Run("Cleanup", helper.testCleanupSearch)
	}
}

func (nqh *searchTestHelper) testSearchBasic(t *testing.T) {
	query := &cbsearchx.TermQuery{
		Term:  "search",
		Field: "service",
	}
	var bucket string
	if nqh.ScopeName != "" {
		bucket = testutils.TestOpts.BucketName
	}
	var result cbsearchx.QueryResultStream
	var rows []*cbsearchx.QueryResultHit
	require.Eventually(t, func() bool {
		var err error
		var min float64 = 30
		var max float64 = 31
		start := "2000-07-22 20:00:20"
		end := "2020-07-22 20:00:20"
		result, err = nqh.Agent.Search(context.Background(), &cbsearchx.QueryOptions{
			IndexName:  nqh.IndexName,
			BucketName: bucket,
			Query:      query,
			ScopeName:  nqh.ScopeName,
			Facets: map[string]cbsearchx.Facet{
				"type": &cbsearchx.TermFacet{Field: "country", Size: 5},
				"date": &cbsearchx.DateFacet{
					Field: "updated",
					Size:  5,
					DateRanges: []cbsearchx.DateFacetRange{
						{
							Name:  "updated",
							Start: &start,
							End:   &end,
						},
					},
				},
				"numeric": &cbsearchx.NumericFacet{
					Field: "geo.lat",
					Size:  5,
					NumericRanges: []cbsearchx.NumericFacetRange{
						{
							Name: "lat",
							Min:  &min,
							Max:  &max,
						},
					},
				},
			},
			IncludeLocations: true,
		})
		if err != nil {
			t.Logf("Failed to query index: %s", err)
			return false
		}

		var thisRows []*cbsearchx.QueryResultHit
		for result.HasMoreHits() {
			row, err := result.ReadHit()
			if err != nil {
				t.Logf("Failed to read row: %s", err)
				return false
			}
			thisRows = append(thisRows, row)
		}

		if len(nqh.QueryTestDocs.docs) == len(thisRows) {
			rows = thisRows
			return true
		}

		t.Logf("Incorrect number of rows, expected: %d, was %d", len(nqh.QueryTestDocs.docs), len(thisRows))
		return false
	}, 120*time.Second, 500*time.Millisecond)

	for _, row := range rows {
		if assert.Contains(t, row.Locations, "service") {
			if assert.Contains(t, row.Locations["service"], "search") {
				if assert.NotZero(t, row.Locations["service"]["search"]) {
					assert.Zero(t, row.Locations["service"]["search"][0].Start)
					assert.NotZero(t, row.Locations["service"]["search"][0].End)
					assert.Nil(t, row.Locations["service"]["search"][0].ArrayPositions)
				}
			}
		}
	}

	metadata, err := result.MetaData()
	require.NoError(t, err)

	assert.NotZero(t, metadata.Metrics.TotalHits)
	assert.NotZero(t, metadata.Metrics.Took)
	assert.NotZero(t, metadata.Metrics.MaxScore)
	assert.NotZero(t, metadata.Metrics.TotalPartitionCount)
	// Not checked due to ING-687
	/*
		assert.Zero(t, metadata.Metrics.FailedPartitionCount)
		assert.NotZero(t, metadata.Metrics.SuccessfulPartitionCount)
	*/

	facets, err := result.Facets()
	require.NoError(t, err)
	if assert.Contains(t, facets, "type") {
		f := facets["type"]
		assert.Equal(t, "country", f.Field)
		assert.Equal(t, uint64(7), f.Total)
		assert.Equal(t, 4, len(f.Terms))
		for _, term := range f.Terms {
			switch term.Term {
			case "belgium":
				assert.Equal(t, 2, term.Count)
			case "states":
				assert.Equal(t, 2, term.Count)
			case "united":
				assert.Equal(t, 2, term.Count)
			case "norway":
				assert.Equal(t, 1, term.Count)
			default:
				t.Fatalf("Unexpected facet term %s", term.Term)
			}
		}
	}

	if assert.Contains(t, facets, "date") {
		f := facets["date"]
		assert.Equal(t, uint64(5), f.Total)
		assert.Equal(t, "updated", f.Field)
		assert.Equal(t, 1, len(f.DateRanges))
		assert.Equal(t, 5, f.DateRanges[0].Count)
		assert.Equal(t, "2000-07-22T20:00:20Z", f.DateRanges[0].Start)
		assert.Equal(t, "2020-07-22T20:00:20Z", f.DateRanges[0].End)
		assert.Equal(t, "updated", f.DateRanges[0].Name)
	}

	if assert.Contains(t, facets, "numeric") {
		f := facets["numeric"]
		assert.Equal(t, uint64(1), f.Total)
		assert.Equal(t, "geo.lat", f.Field)
		assert.Equal(t, 1, len(f.NumericRanges))
		assert.Equal(t, 1, f.NumericRanges[0].Count)
		assert.Equal(t, float64(30), f.NumericRanges[0].Min)
		assert.Equal(t, float64(31), f.NumericRanges[0].Max)
		assert.Equal(t, "lat", f.NumericRanges[0].Name)
	}
}

type searchTestHelper struct {
	TestName       string
	QueryTestDocs  *testBreweryDocs
	Agent          *Agent
	QueryFn        func(context.Context, *cbsearchx.QueryOptions) (cbsearchx.QueryResultStream, error)
	T              *testing.T
	IndexName      string
	ScopeName      string
	CollectionName string
}

func (nqh *searchTestHelper) testSetupSearch(t *testing.T) {
	nqh.QueryTestDocs = makeBreweryTestDocs(context.Background(), t, nqh.Agent, "search", nqh.ScopeName, nqh.CollectionName)
	nqh.IndexName = "a" + uuid.NewString()[:6]

	var bucket string
	if nqh.ScopeName != "" {
		bucket = testutils.TestOpts.BucketName
	}

	err := nqh.Agent.UpsertSearchIndex(context.Background(), &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       nqh.IndexName,
			SourceName: testutils.TestOpts.BucketName,
			SourceType: "couchbase",
			Type:       "fulltext-index",
		},
		ScopeName:  nqh.ScopeName,
		BucketName: bucket,
	})
	require.NoError(t, err)

	err = nqh.Agent.EnsureSearchIndexCreated(context.Background(), &EnsureSearchIndexCreatedOptions{
		ScopeName:  nqh.ScopeName,
		BucketName: bucket,
		IndexName:  nqh.IndexName,
	})
	require.NoError(t, err)
}

func (nqh *searchTestHelper) testCleanupSearch(t *testing.T) {
	if nqh.QueryTestDocs != nil {
		nqh.QueryTestDocs.Remove(context.Background())
		nqh.QueryTestDocs = nil
	}

	var bucket string
	if nqh.ScopeName != "" {
		bucket = testutils.TestOpts.BucketName
	}
	if nqh.IndexName != "" {
		err := nqh.Agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName:  nqh.IndexName,
			ScopeName:  nqh.ScopeName,
			BucketName: bucket,
		})
		require.NoError(t, err)
	}
}
