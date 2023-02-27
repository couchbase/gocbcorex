package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/couchbase/gocbcorex/cbrowstreamerx"
	"go.uber.org/zap"
)

type Query struct {
	HttpClient *http.Client
	Logger     *zap.Logger
	QueryCache *PreparedStatementCache
	UserAgent  string
	Username   string
	Password   string
}

func (qc Query) Query(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	if opts.Statement == "" {
		return nil, InvalidArgumentError{"statement cannot be empty"}
	}
	if opts.Endpoint == "" {
		return nil, InvalidArgumentError{"endpoint cannot be empty"}
	}

	payload, err := opts.toMap(ctx)
	if err != nil {
		return nil, err
	}

	pBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return qc.execute(ctx, opts.Statement, opts.Endpoint, opts.ClientContextID, opts.OnBehalfOf, pBytes)
}

func (qc Query) PreparedQuery(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	if opts.Statement == "" {
		return nil, InvalidArgumentError{"statement cannot be empty"}
	}

	cachedStmt, ok := qc.QueryCache.Get(opts.Statement)

	payload, err := opts.toMap(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		// Attempt to execute our cached query plan
		delete(payload, "statement")
		payload["prepared"] = cachedStmt

		pBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}

		results, err := qc.execute(ctx, cachedStmt, opts.Endpoint, opts.ClientContextID, opts.OnBehalfOf, pBytes)
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

	results, err := qc.execute(ctx, prepareStatement, opts.Endpoint, opts.ClientContextID, opts.OnBehalfOf, pBytes)
	if err != nil {
		return nil, err
	}

	preparedName, err := results.PreparedName()
	if err != nil {
		qc.Logger.Warn("Failed to read prepared name from result", zap.Error(err))
		return results, nil
	}

	qc.QueryCache.Put(opts.Statement, preparedName)

	return results, nil

}

func (qc Query) execute(ctx context.Context, statement, endpoint, clientContextID, onBehalfOf string,
	payload []byte) (*QueryRowReader, error) {
	reqURI := endpoint + "/query/service"
	req, err := http.NewRequestWithContext(ctx, "POST", reqURI, ioutil.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		return nil, QueryError{
			InnerError:      err,
			Statement:       statement,
			Endpoint:        endpoint,
			ClientContextID: clientContextID,
		}
	}
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	header.Set("User-Agent", qc.UserAgent)

	if len(onBehalfOf) > 0 {
		header.Set("cb-on-behalf-of", onBehalfOf)
	}

	req.Header = header

	req.SetBasicAuth(qc.Username, qc.Password)

	resp, err := qc.HttpClient.Do(req)
	if err != nil {
		return nil, QueryError{
			InnerError:      err,
			Statement:       statement,
			Endpoint:        endpoint,
			ClientContextID: clientContextID,
		}
	}

	if resp.StatusCode != 200 {
		respBody, readErr := ioutil.ReadAll(resp.Body)
		if readErr != nil {
			return nil, QueryError{
				InnerError:       InternalError{readErr},
				Statement:        statement,
				HTTPResponseCode: resp.StatusCode,
				Endpoint:         endpoint,
				ClientContextID:  clientContextID,
			}
		}

		rawError, err := parseQueryErrorBody(respBody)
		if err != nil {
			return nil, QueryError{
				InnerError:       InternalError{readErr},
				Statement:        statement,
				HTTPResponseCode: resp.StatusCode,
				Endpoint:         endpoint,
				ClientContextID:  clientContextID,
			}
		}
		if len(rawError.Errors) == 0 {
			return nil, QueryError{
				InnerError:       InternalError{errors.New("received a non-200 status but body contained no errors")},
				Statement:        statement,
				HTTPResponseCode: resp.StatusCode,
				Endpoint:         endpoint,
				ClientContextID:  clientContextID,
			}
		}

		queryErr := parseQueryError(rawError)
		queryErr.Statement = statement
		queryErr.HTTPResponseCode = resp.StatusCode
		queryErr.Endpoint = endpoint
		queryErr.ClientContextID = clientContextID

		return nil, queryErr
	}

	streamer, err := cbrowstreamerx.NewQueryStreamer(resp.Body, "results", qc.Logger)
	if err != nil {
		_, readErr := ioutil.ReadAll(resp.Body)
		if readErr != nil {
			qc.Logger.Debug("Failed to read response body", zap.Error(readErr))
		}

		return nil, QueryError{
			InnerError:       InternalError{err},
			Statement:        statement,
			HTTPResponseCode: resp.StatusCode,
			Endpoint:         endpoint,
			ClientContextID:  clientContextID,
		}
	}

	return &QueryRowReader{
		streamer:        streamer,
		endpoint:        endpoint,
		statement:       statement,
		statusCode:      resp.StatusCode,
		clientContextID: clientContextID,
		logger:          qc.Logger,
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
