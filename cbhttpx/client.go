package cbhttpx

import (
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex/contrib/leakcheck"
)

type Client struct {
	Transport http.RoundTripper
}

func (c Client) GetHttpClient() *http.Client {
	return &http.Client{
		Transport: c.Transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// All that we're doing here is setting auth on any redirects.
			// For that reason we can just pull it off the oldest (first) request.
			if len(via) >= 10 {
				// Just duplicate the default behaviour for maximum redirects.
				return errors.New("stopped after 10 redirects")
			}

			oldest := via[0]
			auth := oldest.Header.Get("Authorization")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			return nil
		},
	}
}

func (c Client) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.GetHttpClient().Do(req)
	if err != nil {
		return resp, err
	}

	return leakcheck.WrapHttpResponse(resp), err
}
