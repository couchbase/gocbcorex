package cbqueryx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type queryRespReaderOptions struct {
	Logger          *zap.Logger
	Endpoint        string
	Statement       string
	ClientContextId string
}

type queryRespReader struct {
	logger          *zap.Logger
	endpoint        string
	statement       string
	clientContextId string
	statusCode      int

	stream        io.ReadCloser
	streamer      cbhttpx.RawJsonRowStreamer
	earlyMetaData *QueryEarlyMetaData
	metaData      *QueryMetaData
	metaDataErr   error
}

func newQueryRespReader(resp *http.Response, opts *queryRespReaderOptions) (*queryRespReader, error) {
	r := &queryRespReader{
		logger:          opts.Logger,
		endpoint:        opts.Endpoint,
		statement:       opts.Statement,
		clientContextId: opts.ClientContextId,
		statusCode:      resp.StatusCode,
	}

	err := r.init(resp)
	if err != nil {
		return nil, &QueryError{
			Cause:           err,
			StatusCode:      resp.StatusCode,
			Endpoint:        r.endpoint,
			Statement:       r.statement,
			ClientContextId: r.clientContextId,
		}
	}

	return r, nil
}

func (r *queryRespReader) init(resp *http.Response) error {
	if resp.StatusCode != 200 {
		errBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return &contextualError{
				Description: "non-200 status code received but reading body failed",
				Cause:       err,
			}
		}

		var respJson queryErrorResponseJson
		err = json.Unmarshal(errBody, &respJson)
		if err != nil {
			return contextualError{
				Description: "non-200 status code received but parsing error response body failed",
				Cause:       err,
			}
		}

		return r.parseErrors(respJson.Errors)
	}

	r.stream = resp.Body
	r.streamer = cbhttpx.RawJsonRowStreamer{
		Decoder:    json.NewDecoder(resp.Body),
		RowsAttrib: "results",
	}

	err := r.readEarlyMetaData()
	if err != nil {
		return err
	}

	if !r.streamer.HasMoreRows() {
		err := r.readFinalMetaData()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *queryRespReader) parseErrors(errsJson []*queryErrorJson) *QueryServerErrors {
	var queryErrs []*QueryServerError
	for _, errJson := range errsJson {
		queryErrs = append(queryErrs, r.parseError(errJson))
	}

	return &QueryServerErrors{
		Errors: queryErrs,
	}
}

func (r *queryRespReader) parseError(errJson *queryErrorJson) *QueryServerError {
	var err error

	errCode := errJson.Code
	errCodeGroup := errCode / 1000

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
	if errCode == 1080 {
		err = ErrTimeout
	}
	if errCode == 3000 {
		err = ErrParsingFailure
	}
	if errCode == 12009 {
		err = ErrDmlFailure

		if len(errJson.Reason) > 0 {
			if code, ok := errJson.Reason["code"]; ok {
				code = int(code.(float64))
				if code == 12033 {
					err = ErrCasMismatch
				} else if code == 17014 {
					err = ErrDocumentNotFound
				} else if code == 17012 {
					err = ErrDocumentExists
				}
			}
		}

		if strings.Contains(strings.ToLower(errJson.Msg), "cas mismatch") {
			err = ErrCasMismatch
		}
	}
	if errCode == 13014 {
		err = ErrAuthenticationFailure
	}
	if errCodeGroup == 10 {
		err = ErrAuthenticationFailure
	}

	if err == nil {
		err = errors.New("unexpected query error")
	}

	return &QueryServerError{
		InnerError: err,
		Code:       errJson.Code,
		Msg:        errJson.Msg,
	}
}

func (r *queryRespReader) parseWarnings(warnsJson []*queryWarningJson) []QueryWarning {
	var warns []QueryWarning
	for _, warnJson := range warnsJson {
		warns = append(warns, QueryWarning{
			Code:    warnJson.Code,
			Message: warnJson.Message,
		})
	}
	return warns
}

func (r *queryRespReader) parseMetrics(metricsJson *queryMetricsJson) *QueryMetrics {
	elapsedTime, err := time.ParseDuration(metricsJson.ElapsedTime)
	if err != nil {
		r.logger.Debug("failed to parse query metrics elapsed time",
			zap.Error(err))
	}

	executionTime, err := time.ParseDuration(metricsJson.ExecutionTime)
	if err != nil {
		r.logger.Debug("failed to parse query metrics execution time",
			zap.Error(err))
	}

	return &QueryMetrics{
		ElapsedTime:   elapsedTime,
		ExecutionTime: executionTime,
		ResultCount:   metricsJson.ResultCount,
		ResultSize:    metricsJson.ResultSize,
		MutationCount: metricsJson.MutationCount,
		SortCount:     metricsJson.SortCount,
		ErrorCount:    metricsJson.ErrorCount,
		WarningCount:  metricsJson.WarningCount,
	}
}

func (r *queryRespReader) parseEarlyMetaData(metaDataJson *queryEarlyMetaDataJson) *QueryEarlyMetaData {
	return &QueryEarlyMetaData{
		Prepared: metaDataJson.Prepared,
	}
}

func (r *queryRespReader) parseMetaData(metaDataJson *queryMetaDataJson) (*QueryMetaData, error) {
	if len(metaDataJson.Errors) > 0 {
		return nil, r.parseErrors(metaDataJson.Errors)
	}

	metrics := r.parseMetrics(metaDataJson.Metrics)
	warnings := r.parseWarnings(metaDataJson.Warnings)

	return &QueryMetaData{
		QueryEarlyMetaData: *r.parseEarlyMetaData(&metaDataJson.queryEarlyMetaDataJson),
		RequestID:          metaDataJson.RequestID,
		ClientContextID:    metaDataJson.ClientContextID,
		Status:             metaDataJson.Status,
		Metrics:            *metrics,
		Signature:          metaDataJson.Signature,
		Warnings:           warnings,
		Profile:            metaDataJson.Profile,
	}, nil
}

func (r *queryRespReader) readEarlyMetaData() error {
	preludeBytes, err := r.streamer.ReadPrelude()
	if err != nil {
		return err
	}

	var metaDataJson queryEarlyMetaDataJson
	err = json.Unmarshal(preludeBytes, &metaDataJson)
	if err != nil {
		return err
	}

	r.earlyMetaData = r.parseEarlyMetaData(&metaDataJson)
	return nil
}

func (r *queryRespReader) readFinalMetaData() error {
	epilogBytes, err := r.streamer.ReadEpilog()
	if err != nil {
		return err
	}

	// We close the stream so that if there is some extra data on the wire
	// it gets ignored and the stream is properly closed.
	_ = r.stream.Close()

	var metaDataJson queryMetaDataJson
	err = json.Unmarshal(epilogBytes, &metaDataJson)
	if err != nil {
		return err
	}

	metaData, err := r.parseMetaData(&metaDataJson)
	if err != nil {
		return err
	}

	r.metaData = metaData
	return nil
}

func (r *queryRespReader) HasMoreRows() bool {
	return r.streamer.HasMoreRows()
}

func (r *queryRespReader) ReadRow() (json.RawMessage, error) {
	rowData, err := r.streamer.ReadRow()
	if err != nil {
		return nil, &QueryError{
			Cause:           err,
			StatusCode:      r.statusCode,
			Endpoint:        r.endpoint,
			Statement:       r.statement,
			ClientContextId: r.clientContextId,
		}
	}

	if !r.streamer.HasMoreRows() {
		if r.metaData == nil && r.metaDataErr == nil {
			r.metaDataErr = r.readFinalMetaData()
		}
	}

	return rowData, nil
}

func (r *queryRespReader) EarlyMetaData() *QueryEarlyMetaData {
	return r.earlyMetaData
}

func (r *queryRespReader) MetaData() (*QueryMetaData, error) {
	if r.metaData == nil && r.metaDataErr == nil {
		return nil, errors.New("cannot read meta-data until after all rows are read")
	}

	if r.metaDataErr != nil {
		return nil, &QueryError{
			Cause:           r.metaDataErr,
			StatusCode:      r.statusCode,
			Endpoint:        r.endpoint,
			Statement:       r.statement,
			ClientContextId: r.clientContextId,
		}
	}

	return r.metaData, nil
}
