package cbsearchx

import "encoding/json"

// Facet represents a facet for a search query.
type Facet interface {
	isSearchFacet()
	encodeToJSON() (json.RawMessage, error)
}

var _ Facet = (*NumericFacet)(nil)
var _ Facet = (*DateFacet)(nil)
var _ Facet = (*TermFacet)(nil)

// TermFacet is a search term facet.
type TermFacet struct {
	Field string
	Size  uint64
}

func (f *TermFacet) isSearchFacet() {}

func (f *TermFacet) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := make(map[string]json.RawMessage)
	if f.Field != "" {
		m["field"] = encoder.EncodeField(f.Field)
	}
	if f.Size > 0 {
		m["size"] = encoder.EncodeField(f.Size)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

type NumericFacetRange struct {
	Name string
	Min  *float64
	Max  *float64
}

// NumericFacet is a search numeric range facet.
type NumericFacet struct {
	Field         string
	Size          uint64
	NumericRanges []NumericFacetRange
}

func (f *NumericFacet) isSearchFacet() {}

func (f *NumericFacet) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := make(map[string]json.RawMessage)
	if f.Field != "" {
		m["field"] = encoder.EncodeField(f.Field)
	}
	if f.Size > 0 {
		m["size"] = encoder.EncodeField(f.Size)
	}

	if len(f.NumericRanges) > 0 {
		ranges := make([]map[string]json.RawMessage, len(f.NumericRanges))
		for i, nr := range f.NumericRanges {
			r := map[string]json.RawMessage{
				"name": encoder.EncodeField(nr.Name),
			}
			if nr.Min != nil {
				r["min"] = encoder.EncodeField(*nr.Min)
			}
			if nr.Max != nil {
				r["max"] = encoder.EncodeField(*nr.Max)
			}
			ranges[i] = r
		}
		m["numeric_ranges"] = encoder.EncodeField(ranges)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

type DateFacetRange struct {
	Name  string
	Start *string
	End   *string
}

// DateFacet is a search date range facet.
type DateFacet struct {
	Field      string
	Size       uint64
	DateRanges []DateFacetRange
}

func (f *DateFacet) isSearchFacet() {}

func (f *DateFacet) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := make(map[string]json.RawMessage)
	if f.Field != "" {
		m["field"] = encoder.EncodeField(f.Field)
	}
	if f.Size > 0 {
		m["size"] = encoder.EncodeField(f.Size)
	}

	if len(f.DateRanges) > 0 {
		ranges := make([]map[string]json.RawMessage, len(f.DateRanges))
		for i, nr := range f.DateRanges {
			r := map[string]json.RawMessage{
				"name": encoder.EncodeField(nr.Name),
			}
			if nr.Start != nil {
				r["start"] = encoder.EncodeField(*nr.Start)
			}
			if nr.End != nil {
				r["end"] = encoder.EncodeField(*nr.End)
			}
			ranges[i] = r
		}
		m["date_ranges"] = encoder.EncodeField(ranges)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}
