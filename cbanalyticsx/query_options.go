package cbanalyticsx

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
)

type ScanConsistency string

const (
	ScanConsistencyUnset       ScanConsistency = ""
	ScanConsistencyNotBounded  ScanConsistency = "not_bounded"
	ScanConsistencyRequestPlus ScanConsistency = "request_plus"
)

type QueryOptions struct {
	ClientContextId string
	Priority        int
	ReadOnly        bool
	ScanConsistency ScanConsistency
	Statement       string

	NamedArgs map[string]json.RawMessage
	Raw       map[string]json.RawMessage

	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (o *QueryOptions) encodeToJson() (json.RawMessage, error) {
	var anyErr error

	m := make(map[string]json.RawMessage)

	encodeField := func(val interface{}) json.RawMessage {
		// if any previous error occured, just skip this encoding
		if anyErr != nil {
			return nil
		}

		if duration, isDuration := val.(time.Duration); isDuration {
			// Query expects duration in Go duration string format.
			val = duration.String()
		}

		// attempt to encode the field
		bytes, err := json.Marshal(val)
		if err != nil {
			anyErr = err
			return nil
		}

		return bytes
	}

	if o.ClientContextId != "" {
		m["client_context_id"] = encodeField(o.ClientContextId)
	}
	// o.Priority -- This is a header not a body field
	if o.ReadOnly {
		m["readonly"] = encodeField(true)
	}
	if o.ScanConsistency != ScanConsistencyUnset {
		m["scan_consistency"] = encodeField(o.ScanConsistency)
	}
	if o.Statement != "" {
		m["statement"] = encodeField(o.Statement)
	}

	for k, v := range o.NamedArgs {
		m["$"+k] = v
	}

	for k, v := range o.Raw {
		m[k] = v
	}

	if anyErr != nil {
		return nil, anyErr
	}

	return json.Marshal(m)
}
