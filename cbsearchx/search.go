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
}

func (h Search) NewRequest(
	ctx context.Context,
	method, path, contentType, onBehalfOf string, body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent:     h.UserAgent,
		Endpoint:      h.Endpoint,
		BasicAuthUser: h.Username,
		BasicAuthPass: h.Password,
	}.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
}

func (h Search) Execute(ctx context.Context, method, path, contentType, onBehalfOf string, headers map[string]string, body io.Reader) (*http.Response, error) {
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

	reqURI := fmt.Sprintf("/api/index/%s/query", opts.IndexName)

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
	OnBehalfOf string
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

	iJson, err := h.encodeIndex(&opts.Index)
	if err != nil {
		return err
	}

	resp, err := h.Execute(
		ctx,
		"PUT",
		fmt.Sprintf("/api/index/%s", opts.Name),
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
	IndexName  string
	OnBehalfOf string
}

func (h Search) DeleteIndex(
	ctx context.Context,
	opts *DeleteIndexOptions,
) error {
	if opts.IndexName == "" {
		return errors.New("must specify index name when deleting an index")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/api/index/%s", opts.IndexName),
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
