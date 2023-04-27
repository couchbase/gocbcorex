package cbsearchx

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
)

// HighlightStyle indicates the type of highlighting to use for a search query.
type HighlightStyle string

const (
	// DefaultHighlightStyle specifies to use the default to highlight search result hits.
	DefaultHighlightStyle HighlightStyle = ""

	// HTMLHighlightStyle specifies to use HTML tags to highlight search result hits.
	HTMLHighlightStyle HighlightStyle = "html"

	// AnsiHightlightStyle specifies to use ANSI tags to highlight search result hits.
	AnsiHightlightStyle HighlightStyle = "ansi"
)

// ConsistencyLevel indicates the level of data consistency desired for a search query.
type ConsistencyLevel uint

const (
	consistencyLevelNotSet ConsistencyLevel = iota

	// ConsistencyLevelNotBounded indicates no data consistency is required.
	ConsistencyLevelNotBounded

	// ConsistencyLevelAtPlus indicates at_plus consistency is required.
	ConsistencyLevelAtPlus
)

type ConsistencyResults string

const (
	ConsistencyResultsUnset    ConsistencyResults = ""
	ConsistencyResultsComplete ConsistencyResults = "complete"
)

// Highlight are the options available for search highlighting.
type Highlight struct {
	Style  HighlightStyle
	Fields []string
}

type Consistency struct {
	Level   ConsistencyLevel
	Results ConsistencyResults
	Vectors map[string]map[string]uint64
}

type Control struct {
	Consistency *Consistency
	Timeout     time.Duration
}

type QueryOptions struct {
	Collections      []string
	Control          *Control
	Explain          bool
	Facets           map[string]Facet
	Fields           []string
	From             int
	Highlight        *Highlight
	IncludeLocations bool
	Query            Query
	Score            string
	SearchAfter      []string
	SearchBefore     []string
	Size             int
	Sort             []Sort

	IndexName  string
	ScopeName  string
	BucketName string

	Raw map[string]json.RawMessage

	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (o *QueryOptions) encodeToJson() (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}

	query, err := o.Query.encodeToJSON()
	if err != nil {
		return nil, err
	}
	m := map[string]json.RawMessage{
		"query": query,
	}
	if len(o.Collections) > 0 {
		m["collections"] = encoder.EncodeField(o.Collections)
	}
	if o.Control != nil {
		control := make(map[string]json.RawMessage)
		if o.Control.Consistency != nil {
			consistency := make(map[string]json.RawMessage)
			switch o.Control.Consistency.Level {
			case consistencyLevelNotSet:
			case ConsistencyLevelNotBounded:
				consistency["level"] = encoder.EncodeField("")
			case ConsistencyLevelAtPlus:
				consistency["level"] = encoder.EncodeField("at_plus")
			}
			if o.Control.Consistency.Results != ConsistencyResultsUnset {
				consistency["results"] = encoder.EncodeField(o.Control.Consistency.Results)
			}
			if len(o.Control.Consistency.Vectors) > 0 {
				consistency["vectors"] = encoder.EncodeField(o.Control.Consistency.Vectors)
			}
			control["consistency"] = encoder.EncodeField(consistency)
		}
		if o.Control.Timeout > 0 {
			control["timeout"] = encoder.EncodeField(o.Control.Timeout)
		}
		m["ctl"] = encoder.EncodeField(control)
	}
	if o.Explain {
		m["explain"] = encoder.EncodeField(o.Explain)
	}
	if len(o.Facets) > 0 {
		facets := make(map[string]json.RawMessage, len(o.Facets))
		for k, facet := range o.Facets {
			raw, err := facet.encodeToJSON()
			if err != nil {
				return nil, err
			}

			facets[k] = raw
		}
		m["facets"] = encoder.EncodeField(facets)
	}
	if len(o.Fields) > 0 {
		m["fields"] = encoder.EncodeField(o.Fields)
	}
	if o.From > 0 {
		m["from"] = encoder.EncodeField(o.From)
	}
	if o.Highlight != nil {
		highlight := map[string]json.RawMessage{
			"fields": encoder.EncodeField(o.Highlight.Fields),
		}
		if o.Highlight.Style != "" {
			highlight["style"] = encoder.EncodeField(o.Highlight.Style)
		}
		m["highlight"] = encoder.EncodeField(highlight)
	}
	if o.IncludeLocations {
		m["includeLocations"] = encoder.EncodeField(o.IncludeLocations)
	}
	if o.Score != "" {
		m["score"] = encoder.EncodeField(o.Score)
	}
	if len(o.SearchAfter) > 0 {
		m["search_after"] = encoder.EncodeField(o.SearchAfter)
	}
	if len(o.SearchBefore) > 0 {
		m["search_before"] = encoder.EncodeField(o.SearchBefore)
	}
	if o.Size > 0 {
		m["size"] = encoder.EncodeField(o.Size)
	}
	if len(o.Sort) > 0 {
		sorts := make([]json.RawMessage, len(o.Sort))
		for i, s := range o.Sort {
			raw, err := s.encodeToJSON()
			if err != nil {
				return nil, err
			}

			sorts[i] = raw
		}
		m["sort"] = encoder.EncodeField(sorts)
	}

	for k, v := range o.Raw {
		m[k] = v
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

type jsonRawMessageEncoder struct {
	anyErr error
}

func (enc *jsonRawMessageEncoder) EncodeField(val interface{}) json.RawMessage {
	// if any previous error occurred, just skip this encoding
	if enc.anyErr != nil {
		return nil
	}

	// attempt to encode the field
	bytes, err := json.Marshal(val)
	if err != nil {
		enc.anyErr = err
		return nil
	}

	return bytes
}

func (enc *jsonRawMessageEncoder) Err() error {
	return enc.anyErr
}
