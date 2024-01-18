package cbauthx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type AuthCheckResponse struct {
	User   string `json:"user"`
	Domain string `json:"domain"`
	Uuid   string `json:"uuid"`
}

type AuthCheckHttp struct {
	transport   http.RoundTripper
	uri         string
	clusterUuid string
}

var _ AuthCheck = (*AuthCheckHttp)(nil)

type AuthCheckHttpOptions struct {
	Transport   http.RoundTripper
	Uri         string
	ClusterUuid string
}

func NewAuthCheckHttp(opts *AuthCheckHttpOptions) *AuthCheckHttp {
	return &AuthCheckHttp{
		transport:   opts.Transport,
		uri:         opts.Uri,
		clusterUuid: opts.ClusterUuid,
	}
}

func (a *AuthCheckHttp) CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error) {
	qs := make(url.Values)
	if a.clusterUuid != "" {
		qs.Set("uuid", a.clusterUuid)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", a.uri+"?"+qs.Encode(), nil)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to create request",
			Cause:   err,
		}
	}

	req.SetBasicAuth(username, password)

	resp, err := a.transport.RoundTrip(req)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to execute auth request",
			Cause:   err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		return UserInfo{}, ErrInvalidAuth
	}

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return UserInfo{}, fmt.Errorf("received non-200/401 status code: %s", respBody)
	}

	var jsonResp AuthCheckResponse
	err = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to decode response data",
			Cause:   err,
		}
	}

	// In the interest of mitigating the impact of an incorrect auth check url
	// being used, we validate some expected state of the response which has the
	// effect of significantly reducing the chances we accept auth from those
	// urls which do not actually check auth.
	if jsonResp.User != username {
		return UserInfo{}, errors.New("user field in auth response did not match request")
	}
	if jsonResp.Domain == "" {
		return UserInfo{}, errors.New("domain field missing from auth response")
	}

	return UserInfo{
		Domain: jsonResp.Domain,
		Uuid:   jsonResp.Uuid,
	}, nil
}
