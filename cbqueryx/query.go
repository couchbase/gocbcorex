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
	Logger    *zap.Logger
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Username  string
	Password  string
}

func (h Query) NewRequest(
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

func (h Query) Execute(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	body io.Reader,
) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
	if err != nil {
		return nil, err
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

type ResultStream interface {
	EarlyMetaData() *EarlyMetaData
	HasMoreRows() bool
	ReadRow() (json.RawMessage, error)
	MetaData() (*MetaData, error)
}

func (h Query) Query(ctx context.Context, opts *Options) (ResultStream, error) {
	reqBytes, err := opts.encodeToJson()
	if err != nil {
		return nil, err
	}

	resp, err := h.Execute(ctx, "POST", "/query/service", "application/json", opts.OnBehalfOf, bytes.NewReader(reqBytes))
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
