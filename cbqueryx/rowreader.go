package cbqueryx

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/gocbcorex/cbrowstreamerx"
	"go.uber.org/zap"
)

type QueryRowReader struct {
	streamer        *cbrowstreamerx.QueryStreamer
	endpoint        string
	statement       string
	statusCode      int
	clientContextID string
	logger          *zap.Logger
}

// NextRow reads the next rows bytes from the stream
func (q *QueryRowReader) NextRow() []byte {
	return q.streamer.NextRow()
}

// Err returns any errors that occurred during streaming.
func (q QueryRowReader) Err() error {
	err := q.streamer.Err()
	if err != nil {
		return QueryError{
			InnerError:       InternalError{err},
			Statement:        q.statement,
			HTTPResponseCode: q.statusCode,
			Endpoint:         q.endpoint,
			ClientContextID:  q.clientContextID,
		}
	}

	meta, metaErr := q.streamer.MetaData()
	if metaErr != nil {
		return QueryError{
			InnerError:       InternalError{metaErr},
			Statement:        q.statement,
			HTTPResponseCode: q.statusCode,
			Endpoint:         q.endpoint,
			ClientContextID:  q.clientContextID,
		}
	}

	rawError, err := parseQueryErrorBody(meta)
	if err != nil {
		return QueryError{
			InnerError:       InternalError{err},
			Statement:        q.statement,
			HTTPResponseCode: q.statusCode,
			Endpoint:         q.endpoint,
			ClientContextID:  q.clientContextID,
		}
	}
	if len(rawError.Errors) == 0 {
		return nil
	}

	queryErr := parseQueryError(rawError)
	queryErr.Statement = q.statement
	queryErr.HTTPResponseCode = q.statusCode
	queryErr.Endpoint = q.endpoint
	queryErr.ClientContextID = q.clientContextID

	return queryErr
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryRowReader) MetaData() (*QueryMetaData, error) {
	if r.streamer == nil {
		return nil, r.Err()
	}

	metaDataBytes, err := r.streamer.MetaData()
	if err != nil {
		return nil, err
	}

	var jsonResp jsonQueryMetadata
	err = json.Unmarshal(metaDataBytes, &jsonResp)
	if err != nil {
		return nil, err
	}

	var metaData QueryMetaData
	err = metaData.fromData(jsonResp, r.logger)
	if err != nil {
		return nil, err
	}

	return &metaData, nil
}

// Close immediately shuts down the connection
func (q *QueryRowReader) Close() error {
	return q.streamer.Close()
}

// PreparedName returns the name of the prepared statement created when using enhanced prepared statements.
// If the prepared name has not been seen on the stream then this will return an error.
// Volatile: This API is subject to change.
func (q QueryRowReader) PreparedName() (string, error) {
	val := q.streamer.EarlyMetadata("prepared")
	if val == nil {
		return "", InternalError{errors.New("prepared name not found in metadata")}
	}

	var name string
	err := json.Unmarshal(val, &name)
	if err != nil {
		return "", InternalError{errors.New("failed to parse prepared name")}
	}

	return name, nil
}

// Endpoint returns the address that this query was run against.
// Internal: This should never be used and is not supported.
func (q *QueryRowReader) Endpoint() string {
	return q.endpoint
}
