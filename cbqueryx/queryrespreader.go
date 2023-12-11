package cbqueryx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
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
	earlyMetaData *EarlyMetaData
	metaData      *MetaData
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
	r.logger.Debug("parsing errors from query response",
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

	if errCodeGroup == 4 {
		err = ErrPlanningFailure
	}
	if errCodeGroup == 5 {
		err = ErrInternalServerError
		lowerMsg := strings.ToLower(errJson.Msg)
		if strings.Contains(lowerMsg, "not enough") &&
			strings.Contains(lowerMsg, "replica") {
			err = ServerInvalidArgError{
				Argument: "NumReplicas",
				Reason:   "not enough indexer nodes to create index with replica count",
			}
		}
		if strings.Contains(lowerMsg, "build already in progress") {
			err = ErrBuildAlreadyInProgress
		}
		if match, matchErr := regexp.MatchString(".*?ndex .*? already exist.*", lowerMsg); matchErr == nil && match {
			err = ErrIndexExists
		}
	}
	if errCodeGroup == 12 || errCodeGroup == 14 {
		err = ErrIndexFailure
	}
	if errCodeGroup == 10 {
		err = ErrAuthenticationFailure
	}

	if errCode == 1000 {
		err = ErrWriteInReadOnlyQuery
	}
	if errCode == 1080 {
		err = ErrTimeout
	}
	if errCode == 3000 {
		err = ErrParsingFailure
	}
	if errCode == 4040 || errCode == 4050 || errCode == 4060 || errCode == 4070 || errCode == 4080 || errCode == 4090 {
		err = ErrPreparedStatementFailure
	}
	if errCode == 4300 {
		err = createResourceError(errJson.Msg, ErrIndexExists)
	}
	if errCode == 12003 {
		err = createResourceError(errJson.Msg, ErrCollectionNotFound)
	}
	if errCode == 12004 {
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
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
	if errCode == 12016 {
		err = createResourceError(errJson.Msg, ErrIndexNotFound)
	}

	if errCode == 12021 {
		err = createResourceError(errJson.Msg, ErrScopeNotFound)
	}

	if errCode == 13014 {
		err = createResourceError(errJson.Msg, ErrAuthenticationFailure)
	}

	if err == nil {
		err = errors.New("unexpected query error")
	}

	return &ServerError{
		InnerError: err,
		Code:       errJson.Code,
		Msg:        errJson.Msg,
	}
}

func createResourceError(msg string, cause error) ResourceError {
	err := ResourceError{
		Cause: cause,
	}

	if errors.Is(err, ErrScopeNotFound) || errors.Is(err, ErrCollectionNotFound) {
		parseResourceNotFoundMsg(&err, msg)
	}

	if errors.Is(err, ErrAuthenticationFailure) {
		parseAuthFailureMsg(&err, msg)
	}

	if errors.Is(err, ErrIndexNotFound) || errors.Is(err, ErrIndexExists) {
		parseIndexNotFoundOrExistsMsg(&err, msg)
	}

	return err
}

func parseIndexNotFoundOrExistsMsg(err *ResourceError, msg string) {
	fields := strings.Fields(msg)
	// msg for not found is of the form - "Index Not Found - cause: GSI index testingIndex not found."
	// msg for index exists is of the form - "The index NewIndex already exists."
	for i, f := range fields {
		if f == "index" {
			err.IndexName = fields[i+1]
			return
		}
	}
}

func parseResourceNotFoundMsg(err *ResourceError, msg string) {
	var path string
	fields := strings.Fields(msg)
	for _, f := range fields {
		// Resource path is of the form bucket:bucket.scope.collection
		if strings.Contains(f, ".") && strings.Contains(f, ":") {
			path = f
			break
		}
	}

	_, trimmedPath, found := strings.Cut(path, ":")
	if !found {
		return
	}
	fields = strings.Split(trimmedPath, ".")
	if errors.Is(err, ErrScopeNotFound) {
		// Bucket names are the only one that can contain `.`, which is why we need to reconstruct the name if split
		err.BucketName = strings.Join(fields[:len(fields)-1], ".")
		err.ScopeName = fields[len(fields)-1]
		return
	}

	if errors.Is(err, ErrCollectionNotFound) {
		err.BucketName = strings.Join(fields[:len(fields)-2], ".")
		err.ScopeName = fields[len(fields)-2]
		err.CollectionName = fields[len(fields)-1]
	}
}

func parseAuthFailureMsg(err *ResourceError, msg string) {
	var path string
	fields := strings.Fields(msg)
	for _, f := range fields {
		if strings.Contains(f, ":") {
			path = f
			break
		}
	}

	_, trimmedPath, found := strings.Cut(path, ":")
	if !found {
		return
	}

	var scopeAndCol string
	// If the bucket name contains "." then the path needs to be parsed differently. We differentiate between dots in
	// the bucket name and those separating bucket, scope and collection by the bucket name being wrapped in "`" if
	// it contains "."
	if strings.Contains(trimmedPath, "`") {
		// trimmedPath will have the form "`bucket.name`" or "`bucket.name`.scope.collection" so the fist element of fields
		// will be the empty string
		fields := strings.Split(trimmedPath, "`")
		err.BucketName = fields[1]
		if fields[2] == "" {
			return
		}
		// scopeAndCol is of the form ".scope.collection" meaning fields[1] is empty and the names are in fields[1] and
		// fields[2]
		scopeAndCol = fields[2]
		fields = strings.Split(scopeAndCol, ".")
		if len(fields) < 3 {
			return
		}
		err.ScopeName = fields[1]
		err.CollectionName = fields[2]
	} else {
		fields = strings.Split(trimmedPath, ".")
		err.BucketName = fields[0]
		if len(fields) < 3 {
			return
		}
		err.ScopeName = fields[1]
		err.CollectionName = fields[2]
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
		r.logger.Debug("failed to parse query metrics elapsed time",
			zap.Error(err))
	}

	executionTime, err := time.ParseDuration(metricsJson.ExecutionTime)
	if err != nil {
		r.logger.Debug("failed to parse query metrics execution time",
			zap.Error(err))
	}

	return &Metrics{
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

func (r *queryRespReader) parseEarlyMetaData(metaDataJson *queryEarlyMetaDataJson) *EarlyMetaData {
	return &EarlyMetaData{
		Prepared: metaDataJson.Prepared,
	}
}

func (r *queryRespReader) parseMetaData(metaDataJson *queryMetaDataJson) (*MetaData, error) {
	if len(metaDataJson.Errors) > 0 {
		return nil, r.parseErrors(metaDataJson.Errors)
	}

	metrics := r.parseMetrics(metaDataJson.Metrics)
	warnings := r.parseWarnings(metaDataJson.Warnings)

	return &MetaData{
		EarlyMetaData:   *r.parseEarlyMetaData(&metaDataJson.queryEarlyMetaDataJson),
		RequestID:       metaDataJson.RequestID,
		ClientContextID: metaDataJson.ClientContextID,
		Status:          metaDataJson.Status,
		Metrics:         *metrics,
		Signature:       metaDataJson.Signature,
		Warnings:        warnings,
		Profile:         metaDataJson.Profile,
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

func (r *queryRespReader) EarlyMetaData() *EarlyMetaData {
	return r.earlyMetaData
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
