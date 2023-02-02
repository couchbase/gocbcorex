package core

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"sync"

	"go.uber.org/zap"
)

type QueryComponent struct {
	httpCmpt *HTTPComponent
	logger   *zap.Logger

	queryCache map[string]string
	cacheLock  sync.RWMutex
}

type n1qlJSONPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

type QueryRowReader struct {
	streamer   *queryStreamer
	endpoint   string
	statement  string
	statusCode int
}

// NextRow reads the next rows bytes from the stream
func (q *QueryRowReader) NextRow() []byte {
	return q.streamer.NextRow()
}

// Err returns any errors that occurred during streaming.
func (q QueryRowReader) Err() error {
	err := q.streamer.Err()
	if err != nil {
		return err
	}

	// TODO(chvck): errors
	_, metaErr := q.streamer.MetaData()
	if metaErr != nil {
		return metaErr
	}

	// raw, descs, err := parseN1QLError(meta)
	// if err != nil {
	// 	return &N1QLError{
	// 		InnerError:       err,
	// 		Errors:           descs,
	// 		ErrorText:        raw,
	// 		Statement:        q.statement,
	// 		HTTPResponseCode: q.statusCode,
	// 	}
	// }
	// if len(descs) > 0 {
	// 	return &N1QLError{
	// 		InnerError:       errors.New("query error"),
	// 		Errors:           descs,
	// 		ErrorText:        raw,
	// 		Statement:        q.statement,
	// 		HTTPResponseCode: q.statusCode,
	// 	}
	// }

	return nil
}

// MetaData fetches the non-row bytes streamed in the response.
func (q *QueryRowReader) MetaData() ([]byte, error) {
	return q.streamer.MetaData()
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
		return "", placeholderError{"prepared name not found in metadata"}
	}

	var name string
	err := json.Unmarshal(val, &name)
	if err != nil {
		return "", placeholderError{"failed to parse prepared name"}
	}

	return name, nil
}

// Endpoint returns the address that this query was run against.
// Internal: This should never be used and is not supported.
func (q *QueryRowReader) Endpoint() string {
	return q.endpoint
}

func (qc *QueryComponent) Query(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	if opts.Statement == "" {
		return nil, errors.New("statement cannot be empty")
	}

	payload, err := opts.toMap(ctx)
	if err != nil {
		return nil, err
	}

	pBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := &HTTPRequest{
		Service:      QueryService,
		Method:       "POST",
		Path:         "/query/service",
		IsIdempotent: opts.Readonly,
		UniqueID:     opts.ClientContextID,
		Endpoint:     opts.Endpoint,
		Body:         pBytes,
	}

	return qc.execute(ctx, req)
}

func (qc *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	if opts.Statement == "" {
		return nil, errors.New("statement cannot be empty")
	}

	qc.cacheLock.RLock()
	cachedStmt := qc.queryCache[opts.Statement]
	qc.cacheLock.RUnlock()

	payload, err := opts.toMap(ctx)
	if err != nil {
		return nil, err
	}

	if cachedStmt != "" {
		req := &HTTPRequest{
			Service:      QueryService,
			Method:       "POST",
			Path:         "/query/service",
			IsIdempotent: opts.Readonly,
			UniqueID:     opts.ClientContextID,
			Endpoint:     opts.Endpoint,
		}

		// Attempt to execute our cached query plan
		delete(payload, "statement")
		payload["prepared"] = cachedStmt

		pBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}

		req.Body = pBytes

		results, err := qc.execute(ctx, req)
		if err == nil {
			return results, nil
		}

		// if we fail to send the prepared statement name then retry a PREPARE.
		delete(payload, "prepared")
	}

	payload["statement"] = "PREPARE " + opts.Statement
	payload["auto_execute"] = true

	pBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := &HTTPRequest{
		Service:      QueryService,
		Method:       "POST",
		Path:         "/query/service",
		IsIdempotent: opts.Readonly,
		UniqueID:     opts.ClientContextID,
		Endpoint:     opts.Endpoint,
		Body:         pBytes,
	}

	results, err := qc.execute(ctx, req)
	if err != nil {
		return nil, err
	}

	preparedName, err := results.PreparedName()
	if err != nil {
		qc.logger.Warn("Failed to read prepared name from result", zap.Error(err))
		return results, nil
	}

	qc.cacheLock.Lock()
	qc.queryCache[opts.Statement] = preparedName
	qc.cacheLock.Unlock()

	return results, nil

}

// TODO(chvck): error handling
func (qc *QueryComponent) execute(ctx context.Context, req *HTTPRequest) (*QueryRowReader, error) {
	resp, err := qc.httpCmpt.SendHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.Raw.StatusCode != 200 {
		return nil, errors.New("somethingwentwrong")
	}

	streamer, err := newQueryStreamer(resp.Raw.Body, "results", qc.logger)
	if err != nil {
		respBody, readErr := ioutil.ReadAll(resp.Raw.Body)
		if readErr != nil {
			qc.logger.Debug("Failed to read response body", zap.Error(readErr))
		}
		return nil, errors.New(string(respBody))
	}

	return &QueryRowReader{
		streamer: streamer,
		endpoint: resp.Endpoint,
		// statement:  statementForErr,
		statusCode: resp.Raw.StatusCode,
	}, nil
}
