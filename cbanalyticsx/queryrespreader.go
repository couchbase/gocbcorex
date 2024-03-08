package cbanalyticsx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
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

	stream      io.ReadCloser
	streamer    cbhttpx.RawJsonRowStreamer
	metaData    *MetaData
	metaDataErr error
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
		return nil, &Error{
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

func (r *queryRespReader) parseErrors(errsJson []*queryErrorJson) *ServerErrors {
	r.logger.Debug("parsing errors from analytics query response",
		zap.Any("errors", errsJson))

	var queryErrs []*ServerError
	for _, errJson := range errsJson {
		queryErrs = append(queryErrs, r.parseError(errJson))
	}

	return &ServerErrors{
		Errors: queryErrs,
	}
}

func (r *queryRespReader) parseError(errJson *queryErrorJson) *ServerError {
	var err error

	errCode := errJson.Code
	errCodeGroup := errCode / 1000

	if errCodeGroup == 20 {
		err = ErrAuthenticationFailure
	}
	if errCodeGroup == 24 {
		err = ErrCompilationFailure
	}
	if errCodeGroup == 25 {
		err = ErrInternalServerError
	}

	if errCode == 23000 || errCode == 23003 {
		err = ErrTemporaryFailure
	}
	if errCode == 23007 {
		err = ErrJobQueueFull
	}
	if errCode == 24000 {
		err = ErrParsingFailure
	}
	if errCode == 24006 {
		err = ErrLinkNotFound
	}
	if errCode == 24025 || errCode == 24044 || errCode == 24045 {
		err = ErrDatasetNotFound
	}
	if errCode == 24034 {
		err = ErrDataverseNotFound
	}
	if errCode == 24039 {
		err = ErrJobQueueFull
	}
	if errCode == 24040 {
		err = ErrDatasetExists
	}
	if errCode == 24047 {
		err = ErrIndexNotFound
	}
	if errCode == 24048 {
		err = ErrIndexExists
	}

	if err == nil {
		err = errors.New("unexpected analytics query error")
	}

	return &ServerError{
		InnerError: err,
		Code:       errJson.Code,
		Msg:        errJson.Msg,
	}
}

func (r *queryRespReader) parseWarnings(warnsJson []*queryWarningJson) []Warning {
	var warns []Warning
	for _, warnJson := range warnsJson {
		warns = append(warns, Warning{
			Code:    warnJson.Code,
			Message: warnJson.Message,
		})
	}
	return warns
}

func (r *queryRespReader) parseMetrics(metricsJson *queryMetricsJson) *Metrics {
	elapsedTime, err := time.ParseDuration(metricsJson.ElapsedTime)
	if err != nil {
		r.logger.Debug("failed to parse analytics query metrics elapsed time",
			zap.Error(err))
	}

	executionTime, err := time.ParseDuration(metricsJson.ExecutionTime)
	if err != nil {
		r.logger.Debug("failed to parse analytics query metrics execution time",
			zap.Error(err))
	}

	return &Metrics{
		ElapsedTime:      elapsedTime,
		ExecutionTime:    executionTime,
		ResultCount:      metricsJson.ResultCount,
		ResultSize:       metricsJson.ResultSize,
		MutationCount:    metricsJson.MutationCount,
		SortCount:        metricsJson.SortCount,
		ErrorCount:       metricsJson.ErrorCount,
		WarningCount:     metricsJson.WarningCount,
		ProcessedObjects: metricsJson.ProcessedObjects,
	}
}

func (r *queryRespReader) parseMetaData(metaDataJson *queryMetaDataJson) (*MetaData, error) {
	metrics := r.parseMetrics(metaDataJson.Metrics)
	warnings := r.parseWarnings(metaDataJson.Warnings)

	return &MetaData{
		RequestID:       metaDataJson.RequestID,
		ClientContextID: metaDataJson.ClientContextID,
		Status:          metaDataJson.Status,
		Metrics:         *metrics,
		Warnings:        warnings,
		Signature:       metaDataJson.Signature,
	}, nil
}

func (r *queryRespReader) readEarlyMetaData() error {
	// we read the prelude, but ignore it since we don't need
	// any early meta-data with analytics
	_, err := r.streamer.ReadPrelude()
	if err != nil {
		return err
	}

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
		return nil, &Error{
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

func (r *queryRespReader) MetaData() (*MetaData, error) {
	if r.metaData == nil && r.metaDataErr == nil {
		return nil, errors.New("cannot read meta-data until after all rows are read")
	}

	if r.metaDataErr != nil {
		return nil, &Error{
			Cause:           r.metaDataErr,
			StatusCode:      r.statusCode,
			Endpoint:        r.endpoint,
			Statement:       r.statement,
			ClientContextId: r.clientContextId,
		}
	}

	return r.metaData, nil
}
