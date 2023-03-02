package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type Query struct {
	Logger     *zap.Logger
	Transport  http.RoundTripper
	UserAgent  string
	Endpoint   string
	Username   string
	Password   string
	OnBehalfOf string
}

func (h Query) NewRequest(
	ctx context.Context,
	method string, path string,
	contentType string, body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent:     h.UserAgent,
		Endpoint:      h.Endpoint,
		BasicAuthUser: h.Username,
		BasicAuthPass: h.Password,
		CbOnBehalfOf:  h.OnBehalfOf,
	}.NewRequest(ctx, method, path, contentType, body)
}

func (h Query) Execute(ctx context.Context, method string, path string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, body)
	if err != nil {
		return nil, err
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

type QueryResultStream interface {
	EarlyMetaData() *QueryEarlyMetaData
	HasMoreRows() bool
	ReadRow() (json.RawMessage, error)
	MetaData() (*QueryMetaData, error)
}

func (h Query) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	reqBytes, err := opts.encodeToJson()
	if err != nil {
		return nil, err
	}

	resp, err := h.Execute(ctx, "POST", "/query/service", "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		return nil, err
	}

	return newQueryRespReader(resp, &queryRespReaderOptions{
		Logger:          h.Logger,
		Endpoint:        h.Endpoint,
		Statement:       opts.Statement,
		ClientContextId: opts.ClientContextId,
	})
}
