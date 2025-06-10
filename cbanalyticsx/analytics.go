package cbanalyticsx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type Analytics struct {
	Logger    *zap.Logger
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Username  string
	Password  string
}

func (h Analytics) NewRequest(
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

func (h Analytics) Execute(
	ctx context.Context,
	method, path, contentType string,
	extraHeaders map[string]string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	body io.Reader,
) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
	if err != nil {
		return nil, err
	}

	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

type QueryResultStream interface {
	HasMoreRows() bool
	ReadRow() (json.RawMessage, error)
	MetaData() (*MetaData, error)
}

func (h Analytics) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	reqBytes, err := opts.encodeToJson()
	if err != nil {
		return nil, err
	}

	var extraHeaders map[string]string
	if opts.Priority != 0 {
		extraHeaders = map[string]string{
			"Analytics-Priority": fmt.Sprintf("%d", opts.Priority),
		}
	}

	resp, err := h.Execute(ctx, "POST", "/analytics/service", "application/json", extraHeaders, opts.OnBehalfOf, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, err
	}

	return NewQueryRespReader(resp, &QueryRespReaderOptions{
		Logger:          h.Logger,
		Endpoint:        h.Endpoint,
		Statement:       opts.Statement,
		ClientContextId: opts.ClientContextId,
	})
}
