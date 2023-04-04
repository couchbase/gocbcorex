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

func (nqh *searchTestHelper) testSearchBasic(t *testing.T) {
	query := &cbsearchx.TermQuery{
		Term:  "search",
		Field: "service",
	}
	var result cbsearchx.QueryResultStream
	var rows []*cbsearchx.QueryResultHit
	require.Eventually(nqh.T, func() bool {
		var err error
		var min float64 = 30
		var max float64 = 31
		start := "2000-07-22 20:00:20"
		end := "2020-07-22 20:00:20"
		result, err = nqh.Agent.Search(context.Background(), &cbsearchx.QueryOptions{
			IndexName: nqh.IndexName,
			Query:     query,
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
			nqh.T.Logf("Failed to query index: %s", err)
			return false
		}

		var thisRows []*cbsearchx.QueryResultHit
		for result.HasMoreHits() {
			row, err := result.ReadHit()
			if err != nil {
				nqh.T.Logf("Failed to read row: %s", err)
				return false
			}
			thisRows = append(thisRows, row)
		}

		if len(nqh.QueryTestDocs.docs) == len(thisRows) {
			rows = thisRows
			return true
		}

		nqh.T.Logf("Incorrect number of rows, expected: %d, was %d", len(nqh.QueryTestDocs.docs), len(thisRows))
		return false
	}, 60*time.Second, 500*time.Millisecond)

	for _, row := range rows {
		if assert.Contains(nqh.T, row.Locations, "service") {
			if assert.Contains(nqh.T, row.Locations["service"], "search") {
				if assert.NotZero(nqh.T, row.Locations["service"]["search"]) {
					assert.Zero(nqh.T, row.Locations["service"]["search"][0].Start)
					assert.NotZero(nqh.T, row.Locations["service"]["search"][0].End)
					assert.Nil(nqh.T, row.Locations["service"]["search"][0].ArrayPositions)
				}
			}
		}
	}

	metadata, err := result.MetaData()
	require.NoError(nqh.T, err)

	assert.NotEmpty(nqh.T, metadata.Metrics.TotalHits)
	assert.NotEmpty(nqh.T, metadata.Metrics.Took)
	assert.NotEmpty(nqh.T, metadata.Metrics.MaxScore)

	facets, err := result.Facets()
	require.NoError(nqh.T, err)
	if assert.Contains(nqh.T, facets, "type") {
		f := facets["type"]
		assert.Equal(nqh.T, "country", f.Field)
		assert.Equal(nqh.T, uint64(7), f.Total)
		assert.Equal(nqh.T, 4, len(f.Terms))
		for _, term := range f.Terms {
			switch term.Term {
			case "belgium":
				assert.Equal(nqh.T, 2, term.Count)
			case "states":
				assert.Equal(nqh.T, 2, term.Count)
			case "united":
				assert.Equal(nqh.T, 2, term.Count)
			case "norway":
				assert.Equal(nqh.T, 1, term.Count)
			default:
				nqh.T.Fatalf("Unexpected facet term %s", term.Term)
			}
		}
	}

	if assert.Contains(nqh.T, facets, "date") {
		f := facets["date"]
		assert.Equal(nqh.T, uint64(5), f.Total)
		assert.Equal(nqh.T, "updated", f.Field)
		assert.Equal(nqh.T, 1, len(f.DateRanges))
		assert.Equal(nqh.T, 5, f.DateRanges[0].Count)
		assert.Equal(nqh.T, "2000-07-22T20:00:20Z", f.DateRanges[0].Start)
		assert.Equal(nqh.T, "2020-07-22T20:00:20Z", f.DateRanges[0].End)
		assert.Equal(nqh.T, "updated", f.DateRanges[0].Name)
	}

	if assert.Contains(nqh.T, facets, "numeric") {
		f := facets["numeric"]
		assert.Equal(nqh.T, uint64(1), f.Total)
		assert.Equal(nqh.T, "geo.lat", f.Field)
		assert.Equal(nqh.T, 1, len(f.NumericRanges))
		assert.Equal(nqh.T, 1, f.NumericRanges[0].Count)
		assert.Equal(nqh.T, float64(30), f.NumericRanges[0].Min)
		assert.Equal(nqh.T, float64(31), f.NumericRanges[0].Max)
		assert.Equal(nqh.T, "lat", f.NumericRanges[0].Name)
	}
}

type searchTestHelper struct {
	TestName      string
	QueryTestDocs *testBreweryDocs
	Agent         *Agent
	QueryFn       func(context.Context, *cbsearchx.QueryOptions) (cbsearchx.QueryResultStream, error)
	T             *testing.T
	IndexName     string
}

func (nqh *searchTestHelper) testSetupSearch(t *testing.T) {
	nqh.QueryTestDocs = makeBreweryTestDocs(context.Background(), t, nqh.Agent, "search")
	nqh.IndexName = "a" + uuid.NewString()[:6]

	err := nqh.Agent.UpsertSearchIndex(context.Background(), &cbsearchx.UpsertIndexOptions{
		Index: cbsearchx.Index{
			Name:       nqh.IndexName,
			SourceName: testutils.TestOpts.BucketName,
			SourceType: "couchbase",
			Type:       "fulltext-index",
		},
	})
	require.NoError(t, err)
}

func (nqh *searchTestHelper) testCleanupSearch(t *testing.T) {
	if nqh.QueryTestDocs != nil {
		nqh.QueryTestDocs.Remove(context.Background())
		nqh.QueryTestDocs = nil
	}
	if nqh.IndexName != "" {
		nqh.Agent.DeleteSearchIndex(context.Background(), &cbsearchx.DeleteIndexOptions{
			IndexName: nqh.IndexName,
		})
	}
}
