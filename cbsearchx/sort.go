package cbsearchx

import "encoding/json"

// Sort represents an search sorting for a search query.
type Sort interface {
	isSearchSort()
	encodeToJSON() (json.RawMessage, error)
}

// SortScore represents a search score sort.
type SortScore struct {
	Descending *bool
}

func (s *SortScore) isSearchFacet() {}

func (s *SortScore) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"by": encoder.EncodeField("score"),
	}
	if s.Descending != nil {
		m["desc"] = encoder.EncodeField(*s.Descending)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// SortID represents a search Document ID sort.
type SortID struct {
	Descending *bool
}

func (s *SortID) isSearchFacet() {}

func (s *SortID) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"by": encoder.EncodeField("id"),
	}
	if s.Descending != nil {
		m["desc"] = encoder.EncodeField(*s.Descending)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// SortField represents a search field sort.
type SortField struct {
	Descending *bool
	Field      string
	Missing    string
	Mode       string
	Type       string
}

func (s *SortField) isSearchFacet() {}

func (s *SortField) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"by": encoder.EncodeField("field"),
	}
	if s.Descending != nil {
		m["desc"] = encoder.EncodeField(*s.Descending)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Missing != "" {
		m["missing"] = encoder.EncodeField(s.Missing)
	}
	if s.Mode != "" {
		m["mode"] = encoder.EncodeField(s.Field)
	}
	if s.Type != "" {
		m["type"] = encoder.EncodeField(s.Type)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

type Location struct {
	Lat float64
	Lon float64
}

// SortGeoDistance represents a search geo sort.
type SortGeoDistance struct {
	Descending *bool
	Field      string
	Location   *Location
	Unit       string
}

func (s *SortGeoDistance) isSearchFacet() {}

func (s *SortGeoDistance) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"by": encoder.EncodeField("geo_distance"),
	}
	if s.Descending != nil {
		m["desc"] = encoder.EncodeField(*s.Descending)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Location != nil {
		m["location"] = encoder.EncodeField([]float64{s.Location.Lon, s.Location.Lat})
	}
	if s.Unit != "" {
		m["unit"] = encoder.EncodeField(s.Unit)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}
