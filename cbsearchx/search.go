package cbsearchx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type Search struct {
	Logger    *zap.Logger
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Username  string
	Password  string

	VectorSearchEnabled bool
}

func (h Search) NewRequest(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent:     h.UserAgent,
		Endpoint:      h.Endpoint,
		BasicAuthUser: h.Username,
		BasicAuthPass: h.Password,
	}.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
}

func (h Search) Execute(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	headers map[string]string,
	body io.Reader,
) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
	if err != nil {
		return nil, err
	}
	for key, header := range headers {
		req.Header.Add(key, header)
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

type QueryResultStream interface {
	HasMoreHits() bool
	ReadHit() (*QueryResultHit, error)
	MetaData() (*MetaData, error)
	Facets() (map[string]FacetResult, error)
}

func (h Search) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	reqBytes, err := opts.encodeToJson()
	if err != nil {
		return nil, err
	}

	if !h.VectorSearchEnabled {
		if len(opts.Knn) > 0 || opts.KnnOperator != KnnOperatorUnset {
			return nil, ErrUnsupportedFeature
		}
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s/query", opts.IndexName)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return nil, errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s/query", opts.BucketName, opts.ScopeName, opts.IndexName)
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		nil,
		bytes.NewReader(reqBytes),
	)
	if err != nil {
		return nil, err
	}

	return newRespReader(resp, &respReaderOptions{
		Logger:   h.Logger,
		Endpoint: h.Endpoint,
	})
}

type UpsertIndexOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
	Index
}

func (h Search) UpsertIndex(
	ctx context.Context,
	opts *UpsertIndexOptions,
) error {
	if opts.Name == "" {
		return errors.New("must specify index name when creating an index")
	}
	if opts.Type == "" {
		return errors.New("must specify index type when creating an index")
	}
	if opts.SourceType == "" {
		return errors.New("must specify source type when creating an index")
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s", opts.Name)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s", opts.BucketName, opts.ScopeName, opts.Name)
	}

	iJson, err := h.encodeIndex(&opts.Index)
	if err != nil {
		return err
	}

	resp, err := h.Execute(
		ctx,
		"PUT",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		map[string]string{"cache-control": "no-cache"},
		bytes.NewReader(iJson))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type DeleteIndexOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) DeleteIndex(
	ctx context.Context,
	opts *DeleteIndexOptions,
) error {
	if opts.IndexName == "" {
		return errors.New("must specify index name when deleting an index")
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s", opts.IndexName)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s", opts.BucketName, opts.ScopeName, opts.IndexName)
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		map[string]string{"cache-control": "no-cache"},
		nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type GetIndexOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) GetIndex(
	ctx context.Context,
	opts *GetIndexOptions,
) (*Index, error) {
	if opts.IndexName == "" {
		return nil, errors.New("must specify index name when getting an index")
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s", opts.IndexName)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return nil, errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s", opts.BucketName, opts.ScopeName, opts.IndexName)
	}

	resp, err := h.Execute(
		ctx,
		"GET",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return nil, h.DecodeCommonError(resp)
	}

	indexData, err := cbhttpx.ReadAsJsonAndClose[searchIndexRespJson](resp.Body)
	if err != nil {
		return nil, err
	}

	idx, err := h.decodeIndex(indexData.IndexDef)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

type GetAllIndexesOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) GetAllIndexes(
	ctx context.Context,
	opts *GetAllIndexesOptions,
) ([]Index, error) {
	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = "/api/index/"
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return nil, errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index", opts.BucketName, opts.ScopeName)
	}

	resp, err := h.Execute(
		ctx,
		"GET",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return nil, h.DecodeCommonError(resp)
	}

	indexData, err := cbhttpx.ReadAsJsonAndClose[searchIndexesRespJson](resp.Body)
	if err != nil {
		return nil, err
	}

	var indexes []Index
	for _, i := range indexData.IndexDefs.IndexDefs {
		idx, err := h.decodeIndex(&i)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

type AnalyzeDocumentOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	DocContent []byte
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type DocumentAnalysis struct {
	Status   string
	Analyzed json.RawMessage
}

func (h Search) AnalyzeDocument(
	ctx context.Context,
	opts *AnalyzeDocumentOptions,
) (*DocumentAnalysis, error) {
	if opts.IndexName == "" {
		return nil, errors.New("must specify index name when analyzing a document")
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s/analyzeDoc", opts.IndexName)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return nil, errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/index/%s.%s.%s/analyzeDoc", opts.BucketName, opts.ScopeName, opts.IndexName)
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		nil,
		bytes.NewReader(opts.DocContent))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return nil, h.DecodeCommonError(resp)
	}

	analysis, err := cbhttpx.ReadAsJsonAndClose[analyzeDocumentJson](resp.Body)
	if err != nil {
		return nil, err
	}

	return &DocumentAnalysis{
		Status:   analysis.Status,
		Analyzed: analysis.Analyzed,
	}, nil
}

type GetIndexedDocumentsCountOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) GetIndexedDocumentsCount(
	ctx context.Context,
	opts *GetIndexedDocumentsCountOptions,
) (uint64, error) {
	if opts.IndexName == "" {
		return 0, errors.New("must specify index name when analyzing a document")
	}

	var reqURI string
	if opts.ScopeName == "" && opts.BucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s/count", opts.IndexName)
	} else {
		if opts.ScopeName == "" || opts.BucketName == "" {
			return 0, errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s/count", opts.BucketName, opts.ScopeName, opts.IndexName)
	}

	resp, err := h.Execute(
		ctx,
		"GET",
		reqURI,
		"application/json",
		opts.OnBehalfOf,
		nil,
		nil)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return 0, h.DecodeCommonError(resp)
	}

	count, err := cbhttpx.ReadAsJsonAndClose[indexedDocumentsJson](resp.Body)
	if err != nil {
		return 0, err
	}

	return count.Count, nil
}

type PauseIngestOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) PauseIngest(
	ctx context.Context,
	opts *PauseIngestOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "ingestControl/pause", opts.OnBehalfOf)
}

type ResumeIngestOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) ResumeIngest(
	ctx context.Context,
	opts *ResumeIngestOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "ingestControl/resume", opts.OnBehalfOf)
}

type AllowQueryingOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) AllowQuerying(
	ctx context.Context,
	opts *AllowQueryingOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "queryControl/allow", opts.OnBehalfOf)
}

type DisallowQueryingOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) DisallowQuerying(
	ctx context.Context,
	opts *DisallowQueryingOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "queryControl/disallow", opts.OnBehalfOf)
}

type FreezePlanOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) FreezePlan(
	ctx context.Context,
	opts *FreezePlanOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "planFreezeControl/freeze", opts.OnBehalfOf)
}

type UnfreezePlanOptions struct {
	BucketName string
	ScopeName  string
	IndexName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) UnfreezePlan(
	ctx context.Context,
	opts *UnfreezePlanOptions,
) error {
	return h.controlRequest(ctx, opts.IndexName, opts.BucketName, opts.ScopeName, "planFreezeControl/unfreeze", opts.OnBehalfOf)
}

func (h Search) controlRequest(
	ctx context.Context,
	indexName, bucketName, scopeName, control string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
) error {
	if indexName == "" {
		return errors.New("must specify index name")
	}

	var reqURI string
	if scopeName == "" && bucketName == "" {
		reqURI = fmt.Sprintf("/api/index/%s/%s", indexName, control)
	} else {
		if scopeName == "" || bucketName == "" {
			return errors.New("must specify both or neither of scope and bucket names")
		}
		reqURI = fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s/%s", bucketName, scopeName, indexName, control)
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		reqURI,
		"application/json",
		onBehalfOf,
		nil,
		nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type RefreshConfigOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Search) RefreshConfig(
	ctx context.Context,
	opts *RefreshConfigOptions,
) error {
	resp, err := h.Execute(
		ctx,
		"POST",
		"/api/cfgRefresh",
		"application/json",
		opts.OnBehalfOf,
		nil,
		nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		return h.DecodeCommonError(resp)
	}

	return nil
}

func (h Search) DecodeCommonError(resp *http.Response) error {
	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return contextualError{
			Description: "failed to read error body for non-success response",
			Cause:       readErr,
		}
	}

	var err error
	errText := strings.ToLower(string(bodyBytes))

	if strings.Contains(errText, "index not found") {
		err = ErrIndexNotFound
	} else if strings.Contains(errText, "index with the same name already exists") {
		err = ErrIndexExists
	} else if strings.Contains(errText, "current index uuid") && strings.Contains(errText, "did not match input uuid") {
		err = ErrIndexExists
	} else if strings.Contains(errText, "unknown indextype") {
		err = ErrUnknownIndexType
	} else if strings.Contains(errText, "error obtaining vbucket count for bucket") ||
		strings.Contains(errText, "requested resource not found") {
		err = ErrSourceNotFound
	} else if strings.Contains(errText, " failed to connect to or retrieve information from source, sourcetype") {
		err = ErrSourceTypeIncorrect
	} else if strings.Contains(errText, "no planpindexes for indexname") {
		err = ErrNoIndexPartitionsPlanned
	} else if strings.Contains(errText, "no local pindexes found") {
		err = ErrNoIndexPartitionsFound
	}

	if err == nil {
		err = errors.New("unexpected error response")
	}

	return SearchError{
		Cause:      err,
		StatusCode: resp.StatusCode,
		Body:       bodyBytes,
		Endpoint:   h.Endpoint,
	}
}

func (h Search) encodeIndex(i *Index) (json.RawMessage, error) {
	encoder := &jsonRawMessageEncoder{}
	m := map[string]json.RawMessage{
		"name": encoder.EncodeField(i.Name),
		"type": encoder.EncodeField(i.Type),
	}
	if len(i.Params) > 0 {
		m["params"] = encoder.EncodeField(i.Params)
	}
	if len(i.PlanParams) > 0 {
		m["planParams"] = encoder.EncodeField(i.PlanParams)
	}
	if i.PrevIndexUUID != "" {
		m["prevIndexUUID"] = encoder.EncodeField(i.PrevIndexUUID)
	}
	if i.SourceName != "" {
		m["sourceName"] = encoder.EncodeField(i.SourceName)
	}
	if len(i.SourceParams) > 0 {
		m["sourceParams"] = encoder.EncodeField(i.SourceParams)
	}
	if i.SourceType != "" {
		m["sourceType"] = encoder.EncodeField(i.SourceType)
	}
	if i.SourceUUID != "" {
		m["sourceUUID"] = encoder.EncodeField(i.SourceUUID)
	}
	if i.UUID != "" {
		m["uuid"] = encoder.EncodeField(i.UUID)
	}

	if err := encoder.Err(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

func (h Search) decodeIndex(i *searchIndexJson) (*Index, error) {
	index := Index{
		Name:         i.Name,
		Params:       i.Params,
		PlanParams:   i.PlanParams,
		SourceName:   i.SourceName,
		SourceParams: i.SourceParams,
		SourceType:   i.SourceType,
		SourceUUID:   i.SourceUUID,
		Type:         i.Type,
		UUID:         i.UUID,
	}

	return &index, nil
}
