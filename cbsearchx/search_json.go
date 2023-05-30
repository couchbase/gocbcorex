package cbsearchx

import "encoding/json"

type searchMetadataStatusJson struct {
	Errors     map[string]string `json:"errors"`
	Failed     uint64            `json:"failed"`
	Successful uint64            `json:"successful"`
	Total      uint64            `json:"total"`
}

type searchEpilogJson struct {
	Status    searchMetadataStatusJson `json:"status,omitempty"`
	TotalHits uint64                   `json:"total_hits"`
	MaxScore  float64                  `json:"max_score"`
	Took      uint64                   `json:"took"`
	Facets    map[string]facetJson     `json:"facets"`
}

type termFacetJson struct {
	Term  string `json:"term,omitempty"`
	Count int    `json:"count,omitempty"`
}

type numericFacetJson struct {
	Name  string  `json:"name,omitempty"`
	Min   float64 `json:"min,omitempty"`
	Max   float64 `json:"max,omitempty"`
	Count int     `json:"count,omitempty"`
}

type dateFacetJson struct {
	Name  string `json:"name,omitempty"`
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
	Count int    `json:"count,omitempty"`
}

type facetJson struct {
	Field         string             `json:"field"`
	Total         uint64             `json:"total"`
	Missing       uint64             `json:"missing"`
	Other         uint64             `json:"other"`
	Terms         []termFacetJson    `json:"terms"`
	NumericRanges []numericFacetJson `json:"numeric_ranges"`
	DateRanges    []dateFacetJson    `json:"date_ranges"`
}

type rowLocationJson struct {
	Field          string   `json:"field"`
	Term           string   `json:"term"`
	Position       uint32   `json:"position"`
	Start          uint32   `json:"start"`
	End            uint32   `json:"end"`
	ArrayPositions []uint32 `json:"array_positions"`
}

type rowLocationsJson map[string]map[string][]rowLocationJson

type rowJson struct {
	Index       string                     `json:"index"`
	ID          string                     `json:"id"`
	Score       float64                    `json:"score"`
	Explanation json.RawMessage            `json:"explanation"`
	Locations   rowLocationsJson           `json:"locations"`
	Fragments   map[string][]string        `json:"fragments"`
	Fields      map[string]json.RawMessage `json:"fields"`
}

type errorResponseJson struct {
	Error string `json:"error"`
}
