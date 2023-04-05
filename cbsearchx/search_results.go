package cbsearchx

import (
	"encoding/json"
	"time"
)

// HitLocation represents the location of a row match
type HitLocation struct {
	ArrayPositions []uint32
	End            uint32
	Position       uint32
	Start          uint32
}

// QueryResultHit represents a single hit returned from a search query.
type QueryResultHit struct {
	Explanation json.RawMessage
	Fields      map[string]json.RawMessage
	Fragments   map[string][]string
	ID          string
	Index       string
	Locations   map[string]map[string][]HitLocation
	Score       float64
}

type MetaData struct {
	Errors  map[string]string
	Metrics Metrics
}

type Metrics struct {
	// ErrorPartitionCount   uint64
	MaxScore float64
	// SuccessPartitionCount uint64
	Took time.Duration
	// TotalPartitionCount   uint64
	TotalHits uint64
}

// TermFacetResult holds the results of a term facet in search results.
type TermFacetResult struct {
	Term  string
	Count int
}

// NumericRangeFacetResult holds the results of a numeric facet in search results.
type NumericRangeFacetResult struct {
	Name  string
	Min   float64
	Max   float64
	Count int
}

// DateRangeFacetResult holds the results of a date facet in search results.
type DateRangeFacetResult struct {
	Name  string
	Start string
	End   string
	Count int
}

// FacetResult provides access to the result of a faceted query.
type FacetResult struct {
	Name          string
	Field         string
	Total         uint64
	Missing       uint64
	Other         uint64
	Terms         []TermFacetResult
	NumericRanges []NumericRangeFacetResult
	DateRanges    []DateRangeFacetResult
}
