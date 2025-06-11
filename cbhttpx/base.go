package cbhttpx

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
)

type OnBehalfOfInfo struct {
	Username string
	Password string
	Domain   string
}

type RequestBuilder struct {
	UserAgent string
	Endpoint  string
	Auth      Authenticator
}

func (h RequestBuilder) NewRequest(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *OnBehalfOfInfo,
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

	if h.Auth != nil {
		h.Auth.ApplyToRequest(req)
	}

	if onBehalfOf != nil {
		if onBehalfOf.Password != "" {
			// If we have the OBO users password, we just directly override the basic auth
			// on the request with those credentials rather than using an on-behalf-of
			// header.  This enables support for older server versions.
			req.SetBasicAuth(onBehalfOf.Username, onBehalfOf.Password)
		} else {
			// Otherwise we send the user/domain using an OBO header.
			oboHdrStr := base64.StdEncoding.EncodeToString(
				[]byte(fmt.Sprintf("%s:%s", onBehalfOf.Username, onBehalfOf.Domain)))
			req.Header.Set("cb-on-behalf-of", oboHdrStr)
		}
	}

	return req, nil
}
