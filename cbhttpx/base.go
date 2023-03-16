package cbhttpx

import (
	"context"
	"io"
	"net/http"
)

type RequestBuilder struct {
	UserAgent     string
	Endpoint      string
	BasicAuthUser string
	BasicAuthPass string
}

func (h RequestBuilder) NewRequest(
	ctx context.Context,
	method, path, contentType, onBehalfOf string,
	body io.Reader,
) (*http.Request, error) {
	uri := h.Endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if h.UserAgent != "" {
		req.Header.Set("User-Agent", h.UserAgent)
	}

	if onBehalfOf != "" {
		req.Header.Set("cb-on-behalf-of", onBehalfOf)
	}

	if h.BasicAuthUser != "" || h.BasicAuthPass != "" {
		req.SetBasicAuth(h.BasicAuthUser, h.BasicAuthPass)
	}

	return req, nil
}
