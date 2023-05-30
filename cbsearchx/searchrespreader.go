package cbsearchx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/cbhttpx"
)

type respReaderOptions struct {
	Logger   *zap.Logger
	Endpoint string
}

type respReader struct {
	Logger     *zap.Logger
	endpoint   string
	statusCode int

	stream    io.ReadCloser
	streamer  cbhttpx.RawJsonRowStreamer
	metaData  *MetaData
	epilogErr error
	facets    map[string]FacetResult
}

func newRespReader(resp *http.Response, opts *respReaderOptions) (*respReader, error) {
	r := &respReader{
		endpoint:   opts.Endpoint,
		statusCode: resp.StatusCode,
	}

	if err := r.init(resp); err != nil {
		return nil, SearchError{
			Cause:      err,
			StatusCode: resp.StatusCode,
			Endpoint:   r.endpoint,
		}
	}

	return r, nil
}

func (r *respReader) init(resp *http.Response) error {
	if resp.StatusCode != 200 {
		errBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return &contextualError{
				Description: "non-200 status code received but reading body failed",
				Cause:       err,
			}
		}

		var respJson errorResponseJson
		err = json.Unmarshal(errBody, &respJson)
		if err != nil {
			return contextualError{
				Description: "non-200 status code received but parsing error response body failed",
				Cause:       err,
			}
		}

		return r.parseError(resp.StatusCode, respJson.Error)
	}

	r.stream = resp.Body
	r.streamer = cbhttpx.RawJsonRowStreamer{
		Decoder:    json.NewDecoder(resp.Body),
		RowsAttrib: "hits",
	}

	_, err := r.streamer.ReadPrelude()
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

func (r *respReader) parseError(statusCode int, errBody string) *ServerError {
	var err error

	if statusCode == 500 {
		err = ErrInternalServerError
	}
	if statusCode == 401 || statusCode == 403 {
		err = ErrAuthenticationFailure
	}
	if statusCode == 400 && strings.Contains(errBody, "index not found") {
		err = ErrIndexNotFound
	}
	// if statusCode == 429 {
	// 	if strings.Contains(errMsg, "num_concurrent_requests") {
	// 		err = errRateLimitedFailure
	// 	} else if strings.Contains(errMsg, "num_queries_per_min") {
	// 		err = errRateLimitedFailure
	// 	} else if strings.Contains(errMsg, "ingress_mib_per_min") {
	// 		err = errRateLimitedFailure
	// 	} else if strings.Contains(errMsg, "egress_mib_per_min") {
	// 		err = errRateLimitedFailure
	// 	}
	// }

	if err == nil {
		err = errors.New("unexpected search query error")
	}

	return &ServerError{
		Cause:      err,
		StatusCode: statusCode,
		Body:       []byte(errBody),
	}
}

func (r *respReader) readFinalMetaData() error {
	epilogBytes, err := r.streamer.ReadEpilog()
	if err != nil {
		return err
	}

	// We close the stream so that if there is some extra data on the wire
	// it gets ignored and the stream is properly closed.
	_ = r.stream.Close()

	var epiJson searchEpilogJson
	err = json.Unmarshal(epilogBytes, &epiJson)
	if err != nil {
		return err
	}

	metaData, err := r.parseMetaData(&epiJson)
	if err != nil {
		return err
	}

	facets, err := r.parseFacets(&epiJson)
	if err != nil {
		return err
	}

	r.metaData = metaData
	r.facets = facets
	return nil
}

func (r *respReader) parseMetaData(metaDataJson *searchEpilogJson) (*MetaData, error) {
	return &MetaData{
		Metrics: Metrics{
			FailedPartitionCount:     metaDataJson.Status.Failed,
			MaxScore:                 metaDataJson.MaxScore,
			SuccessfulPartitionCount: metaDataJson.Status.Successful,
			Took:                     time.Duration(metaDataJson.Took) / time.Nanosecond,
			TotalHits:                metaDataJson.TotalHits,
			TotalPartitionCount:      metaDataJson.Status.Total,
		},
		Errors: metaDataJson.Status.Errors,
	}, nil
}

func (r *respReader) parseFacets(facetsJson *searchEpilogJson) (map[string]FacetResult, error) {
	facets := make(map[string]FacetResult)
	for facetName, facetData := range facetsJson.Facets {
		facet := r.parseFacet(facetData)
		facets[facetName] = facet
	}

	return facets, nil
}

func (r *respReader) parseFacet(data facetJson) FacetResult {
	var result FacetResult
	result.Field = data.Field
	result.Total = data.Total
	result.Missing = data.Missing
	result.Other = data.Other
	for _, term := range data.Terms {
		result.Terms = append(result.Terms, TermFacetResult(term))
	}
	for _, nr := range data.NumericRanges {
		result.NumericRanges = append(result.NumericRanges, NumericRangeFacetResult(nr))
	}
	for _, nr := range data.DateRanges {
		result.DateRanges = append(result.DateRanges, DateRangeFacetResult(nr))
	}

	return result
}

func (r *respReader) parseRow(rowJson rowJson) (*QueryResultHit, error) {
	var row QueryResultHit
	row.Explanation = rowJson.Explanation
	row.Fragments = rowJson.Fragments
	row.Fields = rowJson.Fields
	row.ID = rowJson.ID
	row.Index = rowJson.Index
	row.Score = rowJson.Score

	locations := make(map[string]map[string][]HitLocation)
	for fieldName, fieldData := range rowJson.Locations {
		terms := make(map[string][]HitLocation)
		for termName, termData := range fieldData {
			locations := make([]HitLocation, len(termData))
			for locIdx, locData := range termData {
				var location HitLocation
				location.ArrayPositions = locData.ArrayPositions
				location.End = locData.End
				location.Position = locData.Position
				location.Start = locData.Start
				locations[locIdx] = location
			}
			terms[termName] = locations
		}
		locations[fieldName] = terms
	}
	row.Locations = locations

	return &row, nil
}

func (r *respReader) HasMoreHits() bool {
	return r.streamer.HasMoreRows()
}

func (r *respReader) ReadHit() (*QueryResultHit, error) {
	rowData, err := r.streamer.ReadRow()
	if err != nil {
		return nil, &SearchError{
			Cause:      err,
			StatusCode: r.statusCode,
			Endpoint:   r.endpoint,
		}
	}

	if !r.streamer.HasMoreRows() {
		if r.metaData == nil && r.epilogErr == nil {
			r.epilogErr = r.readFinalMetaData()
		}
	}

	var rowsJson rowJson
	if err := json.Unmarshal(rowData, &rowsJson); err != nil {
		return nil, err
	}

	row, err := r.parseRow(rowsJson)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (r *respReader) MetaData() (*MetaData, error) {
	if r.metaData == nil && r.epilogErr == nil {
		return nil, errors.New("cannot read meta-data until after all rows are read")
	}

	if r.epilogErr != nil {
		return nil, &SearchError{
			Cause:      r.epilogErr,
			StatusCode: r.statusCode,
			Endpoint:   r.endpoint,
		}
	}

	return r.metaData, nil
}

func (r *respReader) Facets() (map[string]FacetResult, error) {
	if r.facets == nil && r.epilogErr == nil {
		return nil, errors.New("cannot read facets until after all rows are read")
	}

	if r.epilogErr != nil {
		return nil, &SearchError{
			Cause:      r.epilogErr,
			StatusCode: r.statusCode,
			Endpoint:   r.endpoint,
		}
	}

	return r.facets, nil
}
