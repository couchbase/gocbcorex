package cbsearchx

import "encoding/json"

// Query represents a search query.
type Query interface {
	isSearchQuery()
	encodeToJSON() (json.RawMessage, error)
}

var _ Query = (*BooleanFieldQuery)(nil)
var _ Query = (*BooleanQuery)(nil)
var _ Query = (*ConjunctionQuery)(nil)
var _ Query = (*DateRangeQuery)(nil)
var _ Query = (*DisjunctionQuery)(nil)
var _ Query = (*DocIDQuery)(nil)
var _ Query = (*GeoBoundingBoxQuery)(nil)
var _ Query = (*GeoDistanceQuery)(nil)
var _ Query = (*GeoPolygonQuery)(nil)
var _ Query = (*MatchAllQuery)(nil)
var _ Query = (*MatchNoneQuery)(nil)
var _ Query = (*MatchPhraseQuery)(nil)
var _ Query = (*MatchQuery)(nil)
var _ Query = (*NumericRangeQuery)(nil)
var _ Query = (*PhraseQuery)(nil)
var _ Query = (*PrefixQuery)(nil)
var _ Query = (*QueryStringQuery)(nil)
var _ Query = (*RegexpQuery)(nil)
var _ Query = (*TermQuery)(nil)
var _ Query = (*TermRangeQuery)(nil)
var _ Query = (*WildcardQuery)(nil)

// MatchOperator defines how the individual match terms should be logically concatenated.
type MatchOperator string

const (
	// MatchOperatorOr specifies that individual match terms are concatenated with a logical OR - this is the default if not provided.
	MatchOperatorOr MatchOperator = "or"

	// MatchOperatorAnd specifies that individual match terms are concatenated with a logical AND.
	MatchOperatorAnd MatchOperator = "and"
)

// MatchQuery represents a search match query.
type MatchQuery struct {
	Analyzer     string
	Boost        float32
	Field        string
	Fuzziness    uint64
	Match        string
	Operator     MatchOperator
	PrefixLength uint64
}

func (s *MatchQuery) isSearchQuery() {}

func (s *MatchQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"match": encoder.EncodeField(s.Match),
	}
	if s.Analyzer != "" {
		m["analyzer"] = encoder.EncodeField(s.Analyzer)
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Fuzziness > 0 {
		m["fuzziness"] = encoder.EncodeField(s.Fuzziness)
	}
	if s.Operator != "" {
		m["operator"] = encoder.EncodeField(s.Operator)
	}
	if s.PrefixLength > 0 {
		m["prefix_length"] = encoder.EncodeField(s.PrefixLength)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// MatchPhraseQuery represents a search match phrase query.
type MatchPhraseQuery struct {
	Analyzer string
	Boost    float32
	Field    string
	Phrase   string
}

func (s *MatchPhraseQuery) isSearchQuery() {}

func (s *MatchPhraseQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"match_phrase": encoder.EncodeField(s.Phrase),
	}
	if s.Analyzer != "" {
		m["analyzer"] = encoder.EncodeField(s.Analyzer)
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// RegexpQuery represents a search regular expression query.
type RegexpQuery struct {
	Boost  float32
	Field  string
	Regexp string
}

func (s *RegexpQuery) isSearchQuery() {}

func (s *RegexpQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"regexp": encoder.EncodeField(s.Regexp),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// QueryStringQuery represents a search string query.
type QueryStringQuery struct {
	Boost float32
	Query string
}

func (s *QueryStringQuery) isSearchQuery() {}

func (s *QueryStringQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"query": encoder.EncodeField(s.Query),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// NumericRangeQuery represents a search numeric range query.
type NumericRangeQuery struct {
	Boost        float32
	Field        string
	InclusiveMin bool
	InclusiveMax bool
	Min          float32
	Max          float32
}

func (s *NumericRangeQuery) isSearchQuery() {}

func (s *NumericRangeQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Min > 0 {
		m["min"] = encoder.EncodeField(s.Min)
		m["inclusive_min"] = encoder.EncodeField(s.InclusiveMin)
	}
	if s.Max > 0 {
		m["max"] = encoder.EncodeField(s.Max)
		m["inclusive_max"] = encoder.EncodeField(s.InclusiveMax)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// DateRangeQuery represents a search date range query.
type DateRangeQuery struct {
	Boost          float32
	DateTimeParser string
	End            string
	Field          string
	InclusiveStart bool
	InclusiveEnd   bool
	Start          string
}

func (s *DateRangeQuery) isSearchQuery() {}

func (s *DateRangeQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.DateTimeParser != "" {
		m["datetime_parser"] = encoder.EncodeField(s.DateTimeParser)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Start != "" {
		m["start"] = encoder.EncodeField(s.Start)
		m["inclusive_start"] = encoder.EncodeField(s.InclusiveStart)
	}
	if s.End != "" {
		m["end"] = encoder.EncodeField(s.End)
		m["inclusive_end"] = encoder.EncodeField(s.InclusiveEnd)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// ConjunctionQuery represents a search conjunction query.
type ConjunctionQuery struct {
	Boost     float32
	Conjuncts []Query
}

func (s *ConjunctionQuery) isSearchQuery() {}

func (s *ConjunctionQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	conjuncts := make([]json.RawMessage, len(s.Conjuncts))
	for i, query := range s.Conjuncts {
		raw, err := query.encodeToJSON()
		if err != nil {
			return nil, err
		}
		conjuncts[i] = raw
	}
	m["conjuncts"] = encoder.EncodeField(conjuncts)

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// DisjunctionQuery represents a search disjunction query.
type DisjunctionQuery struct {
	Boost     float32
	Disjuncts []Query
	Min       uint32
}

func (s *DisjunctionQuery) isSearchQuery() {}

func (s *DisjunctionQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	disjuncts := make([]json.RawMessage, len(s.Disjuncts))
	for i, query := range s.Disjuncts {
		raw, err := query.encodeToJSON()
		if err != nil {
			return nil, err
		}
		disjuncts[i] = raw
	}
	m["disjuncts"] = encoder.EncodeField(disjuncts)
	if s.Min > 0 {
		m["min"] = encoder.EncodeField(s.Min)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// BooleanQuery represents a search boolean query.
type BooleanQuery struct {
	Boost   float32
	Must    *ConjunctionQuery
	MustNot *DisjunctionQuery
	Should  *DisjunctionQuery
}

func (s *BooleanQuery) isSearchQuery() {}

func (s *BooleanQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Must != nil {
		raw, err := s.Must.encodeToJSON()
		if err != nil {
			return nil, err
		}
		m["must"] = raw
	}
	if s.MustNot != nil {
		raw, err := s.MustNot.encodeToJSON()
		if err != nil {
			return nil, err
		}
		m["must_not"] = raw
	}
	if s.Should != nil {
		raw, err := s.Should.encodeToJSON()
		if err != nil {
			return nil, err
		}
		m["should"] = raw
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// WildcardQuery represents a search wildcard query.
type WildcardQuery struct {
	Boost    float32
	Field    string
	Wildcard string
}

func (s *WildcardQuery) isSearchQuery() {}

func (s *WildcardQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"wildcard": encoder.EncodeField(s.Wildcard),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// DocIDQuery represents a search document id query.
type DocIDQuery struct {
	Boost  float32
	DocIds []string
}

func (s *DocIDQuery) isSearchQuery() {}

func (s *DocIDQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"ids": encoder.EncodeField(s.DocIds),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// BooleanFieldQuery represents a search boolean field query.
type BooleanFieldQuery struct {
	Bool  bool
	Boost float32
	Field string
}

func (s *BooleanFieldQuery) isSearchQuery() {}

func (s *BooleanFieldQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"bool": encoder.EncodeField(s.Bool),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// TermQuery represents a search term query.
type TermQuery struct {
	Boost        float32
	Field        string
	Fuzziness    uint64
	PrefixLength uint64
	Term         string
}

func (s *TermQuery) isSearchQuery() {}

func (s *TermQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"term": encoder.EncodeField(s.Term),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Fuzziness > 0 {
		m["fuzziness"] = encoder.EncodeField(s.Fuzziness)
	}
	if s.PrefixLength > 0 {
		m["prefix_length"] = encoder.EncodeField(s.PrefixLength)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// PhraseQuery represents a search phrase query.
type PhraseQuery struct {
	Boost float32
	Field string
	Terms []string
}

func (s *PhraseQuery) isSearchQuery() {}

func (s *PhraseQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"terms": encoder.EncodeField(s.Terms),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// PrefixQuery represents a search prefix query.
type PrefixQuery struct {
	Boost  float32
	Field  string
	Prefix string
}

func (s *PrefixQuery) isSearchQuery() {}

func (s *PrefixQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"prefix": encoder.EncodeField(s.Prefix),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// MatchAllQuery represents a search match all query.
type MatchAllQuery struct {
}

func (s *MatchAllQuery) isSearchQuery() {}

func (s *MatchAllQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"match_all": encoder.EncodeField(nil),
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// MatchNoneQuery represents a search match none query.
type MatchNoneQuery struct {
}

func (s *MatchNoneQuery) isSearchQuery() {}

func (s *MatchNoneQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"match_none": encoder.EncodeField(nil),
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// TermRangeQuery represents a search term range query.
type TermRangeQuery struct {
	Boost        float32
	Field        string
	InclusiveMax bool
	InclusiveMin bool
	Max          string
	Min          string
}

func (s *TermRangeQuery) isSearchQuery() {}

func (s *TermRangeQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	if s.Max != "" {
		m["max"] = encoder.EncodeField(s.Max)
		m["inclusive_max"] = encoder.EncodeField(s.InclusiveMax)
	}
	if s.Min != "" {
		m["min"] = encoder.EncodeField(s.Min)
		m["inclusive_min"] = encoder.EncodeField(s.InclusiveMin)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// GeoDistanceQuery represents a search geographical distance query.
type GeoDistanceQuery struct {
	Distance string
	Boost    float32
	Field    string
	Location Location
}

func (s *GeoDistanceQuery) isSearchQuery() {}

func (s *GeoDistanceQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"location": encoder.EncodeField([]float64{s.Location.Lon, s.Location.Lat}),
		"distance": encoder.EncodeField(s.Distance),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// GeoBoundingBoxQuery represents a search geographical bounding box query.
type GeoBoundingBoxQuery struct {
	BottomRight Location
	Boost       float32
	Field       string
	TopLeft     Location
}

func (s *GeoBoundingBoxQuery) isSearchQuery() {}

func (s *GeoBoundingBoxQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{
		"top_left":     encoder.EncodeField([]float64{s.TopLeft.Lon, s.TopLeft.Lat}),
		"bottom_right": encoder.EncodeField([]float64{s.BottomRight.Lon, s.BottomRight.Lat}),
	}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

// GeoPolygonQuery represents a search query which allows to match inside a geo polygon.
type GeoPolygonQuery struct {
	Boost         float32
	Field         string
	PolygonPoints []Location
}

func (s *GeoPolygonQuery) isSearchQuery() {}

func (s *GeoPolygonQuery) encodeToJSON() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	m := map[string]json.RawMessage{}
	if s.Boost > 0 {
		m["boost"] = encoder.EncodeField(s.Boost)
	}
	if s.Field != "" {
		m["field"] = encoder.EncodeField(s.Field)
	}
	var polyPoints [][]float64
	for _, coord := range s.PolygonPoints {
		polyPoints = append(polyPoints, []float64{coord.Lon, coord.Lat})
	}
	m["polygon_points"] = encoder.EncodeField(polyPoints)

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}
