package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type QueryComponent struct {
	httpCmpt *HTTPComponent
	logger   *zap.Logger

	queryCache map[string]string
	cacheLock  sync.RWMutex
}

type QueryRowReader struct {
	streamer        *queryStreamer
	endpoint        string
	statement       string
	statusCode      int
	clientContextID string
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
			InnerError:       internalError{err},
			Statement:        q.statement,
			HTTPResponseCode: q.statusCode,
			Endpoint:         q.endpoint,
			ClientContextID:  q.clientContextID,
		}
	}

	meta, metaErr := q.streamer.MetaData()
	if metaErr != nil {
		return QueryError{
			InnerError:       internalError{metaErr},
			Statement:        q.statement,
			HTTPResponseCode: q.statusCode,
			Endpoint:         q.endpoint,
			ClientContextID:  q.clientContextID,
		}
	}

	rawError, err := parseQueryErrorBody(meta)
	if err != nil {
		return QueryError{
			InnerError:       internalError{err},
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
		return "", internalError{errors.New("prepared name not found in metadata")}
	}

	var name string
	err := json.Unmarshal(val, &name)
	if err != nil {
		return "", internalError{errors.New("failed to parse prepared name")}
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
		return nil, invalidArgumentError{"statement cannot be empty"}
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

	return qc.execute(ctx, opts.Statement, req)
}

func (qc *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	if opts.Statement == "" {
		return nil, invalidArgumentError{"statement cannot be empty"}
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

		results, err := qc.execute(ctx, cachedStmt, req)
		if err == nil {
			return results, nil
		}

		// if we fail to send the prepared statement name then retry a PREPARE.
		delete(payload, "prepared")
	}

	prepareStatement := "PREPARE " + opts.Statement
	payload["statement"] = prepareStatement
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

	results, err := qc.execute(ctx, prepareStatement, req)
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

func (qc *QueryComponent) execute(ctx context.Context, statement string, req *HTTPRequest) (*QueryRowReader, error) {
	resp, err := qc.httpCmpt.SendHTTPRequest(ctx, req)
	if err != nil {
		return nil, QueryError{
			InnerError:      err,
			Statement:       statement,
			Endpoint:        req.Endpoint,
			ClientContextID: req.UniqueID,
		}
	}

	if resp.Raw.StatusCode != 200 {
		respBody, readErr := ioutil.ReadAll(resp.Raw.Body)
		if readErr != nil {
			return nil, QueryError{
				InnerError:       internalError{readErr},
				Statement:        statement,
				HTTPResponseCode: resp.Raw.StatusCode,
				Endpoint:         resp.Endpoint,
				ClientContextID:  req.UniqueID,
			}
		}

		rawError, err := parseQueryErrorBody(respBody)
		if err != nil {
			return nil, QueryError{
				InnerError:       internalError{err},
				Statement:        statement,
				HTTPResponseCode: resp.Raw.StatusCode,
				Endpoint:         resp.Endpoint,
				ClientContextID:  req.UniqueID,
			}
		}
		if len(rawError.Errors) == 0 {
			return nil, QueryError{
				InnerError:       internalError{errors.New("received a non-200 status but body contained no errors")},
				Statement:        statement,
				HTTPResponseCode: resp.Raw.StatusCode,
				Endpoint:         resp.Endpoint,
				ClientContextID:  req.UniqueID,
			}
		}

		queryErr := parseQueryError(rawError)
		queryErr.Statement = statement
		queryErr.HTTPResponseCode = resp.Raw.StatusCode
		queryErr.Endpoint = resp.Endpoint
		queryErr.ClientContextID = req.UniqueID
	}

	streamer, err := newQueryStreamer(resp.Raw.Body, "results", qc.logger)
	if err != nil {
		_, readErr := ioutil.ReadAll(resp.Raw.Body)
		if readErr != nil {
			qc.logger.Debug("Failed to read response body", zap.Error(readErr))
		}

		return nil, QueryError{
			InnerError:       internalError{err},
			Statement:        statement,
			HTTPResponseCode: resp.Raw.StatusCode,
			Endpoint:         resp.Endpoint,
			ClientContextID:  req.UniqueID,
		}
	}

	return &QueryRowReader{
		streamer:        streamer,
		endpoint:        resp.Endpoint,
		statement:       statement,
		statusCode:      resp.Raw.StatusCode,
		clientContextID: req.UniqueID,
	}, nil
}

type jsonN1QLErrorResponse struct {
	Errors json.RawMessage `json:"errors,omitempty"`
}

type jsonN1QLError struct {
	Code   uint32                 `json:"code"`
	Msg    string                 `json:"msg"`
	Reason map[string]interface{} `json:"reason"`
	Retry  bool                   `json:"retry"`
}

func parseQueryErrorBody(respBody []byte) (jsonN1QLErrorResponse, error) {
	var rawRespParse jsonN1QLErrorResponse
	err := json.Unmarshal(respBody, &rawRespParse)
	if err != nil {
		return jsonN1QLErrorResponse{}, err
	}

	return rawRespParse, nil
}

func parseQueryError(rawRespParse jsonN1QLErrorResponse) QueryError {
	// cause is pulled out of the first error desc that we understand.
	var cause error
	var errorDescs []QueryErrorDesc
	var respParse []jsonN1QLError
	parseErr := json.Unmarshal(rawRespParse.Errors, &respParse)
	if parseErr == nil {
		for _, jsonErr := range respParse {
			errCode := jsonErr.Code
			errCodeGroup := errCode / 1000

			var err error
			if errCodeGroup == 4 {
				err = ErrPlanningFailure
			}
			if errCodeGroup == 5 {
				err = ErrInternalServerError
			}
			if errCodeGroup == 12 || errCodeGroup == 14 && errCode != 12004 && errCode != 12016 {
				err = ErrIndexFailure
			}
			if errCode == 4040 || errCode == 4050 || errCode == 4060 || errCode == 4070 || errCode == 4080 || errCode == 4090 {
				err = ErrPreparedStatementFailure
			}

			// if errCode == 1191 || errCode == 1192 || errCode == 1193 || errCode == 1194 {
			// 	err = errRateLimitedFailure
			// }
			// if errCode == 5000 && strings.Contains(strings.ToLower(firstErr.Message),
			// 	"limit for number of indexes that can be created per scope has been reached") {
			// 	err = errQuotaLimitedFailure
			// }
			// TODO(chvck): Need to consider how to expose this.
			// if errCode == 1080 {
			// 	err = errUnambiguousTimeout
			// }

			if errCode == 3000 {
				err = ErrParsingFailure
			}
			if errCode == 12009 {
				err = extractN1QL12009Error(jsonErr)
			}
			if errCode == 13014 {
				err = ErrAuthenticationFailure
			}
			if errCodeGroup == 10 {
				err = ErrAuthenticationFailure
			}

			if cause == nil && err != nil {
				cause = err
			}

			errorDescs = append(errorDescs, QueryErrorDesc{
				Error:   err,
				Code:    jsonErr.Code,
				Message: jsonErr.Msg,
				Reason:  jsonErr.Reason,
				Retry:   jsonErr.Retry,
			})
		}
	}

	var rawErrors string
	if cause == nil && len(rawRespParse.Errors) > 0 {
		// Only populate if this is an error that we don't recognise.
		rawErrors = string(rawRespParse.Errors)
	}
	if cause == nil {
		cause = errors.New("unknown query error")
	}

	return QueryError{
		InnerError: cause,
		ErrorDescs: errorDescs,
		ErrorsText: rawErrors,
	}
}

func extractN1QL12009Error(err jsonN1QLError) error {
	if len(err.Reason) > 0 {
		if code, ok := err.Reason["code"]; ok {
			code = int(code.(float64))
			if code == 12033 {
				return ErrCasMismatch
			} else if code == 17014 {
				return ErrDocumentNotFound
			} else if code == 17012 {
				return ErrDocumentExists
			}
		}

		return ErrDMLFailure
	}

	if strings.Contains(strings.ToLower(err.Msg), "cas mismatch") {
		return ErrCasMismatch
	}
	return ErrDMLFailure

}
