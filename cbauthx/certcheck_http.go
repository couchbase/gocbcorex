package cbauthx

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type CertCheckResponse struct {
	User   string `json:"user"`
	Domain string `json:"domain"`
	Uuid   string `json:"uuid"`
}

type CertCheckHttp struct {
	transport   http.RoundTripper
	hostPort    string
	uri         string
	clusterUuid string
	username    string
	password    string
}

var _ CertCheck = (*CertCheckHttp)(nil)

type CertCheckHttpOptions struct {
	Transport   http.RoundTripper
	Uri         string
	ClusterUuid string
	Username    string
	Password    string
}

func NewCertCheckHttp(opts *CertCheckHttpOptions) *CertCheckHttp {
	parsedEndpoint, _ := url.Parse(opts.Uri)

	return &CertCheckHttp{
		transport:   opts.Transport,
		hostPort:    parsedEndpoint.Host,
		uri:         opts.Uri,
		clusterUuid: opts.ClusterUuid,
		username:    opts.Username,
		password:    opts.Password,
	}
}

func (a *CertCheckHttp) checkCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	if len(connState.PeerCertificates) == 0 {
		return UserInfo{}, ErrNoCert
	}

	clientCert := connState.PeerCertificates[0]

	req, err := http.NewRequestWithContext(ctx, "POST", a.uri, bytes.NewReader(clientCert.Raw))
	req.SetBasicAuth(a.username, a.password)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to create request",
			Cause:   err,
		}
	}

	resp, err := a.transport.RoundTrip(req)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to execute cert check request",
			Cause:   err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 401 {
		return UserInfo{}, ErrInvalidAuth
	}

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return UserInfo{}, fmt.Errorf("received non-200/401 status code: %s", respBody)
	}

	var authResp UserInfo
	err = json.NewDecoder(resp.Body).Decode(&authResp)
	if err != nil {
		return UserInfo{}, &contextualError{
			Message: "failed to decode response data",
			Cause:   err,
		}
	}

	// In the interest of mitigating the impact of an incorrect cert check url
	// being used, we validate some expected state of the response which has the
	// effect of significantly reducing the chances we accept certs from those
	// urls which do not actually check certs.
	if authResp.User == "" {
		return UserInfo{}, errors.New("user field missing from cert check response")
	}
	if authResp.Domain == "" {
		return UserInfo{}, errors.New("domain field missing from cert check response")
	}

	return authResp, nil
}

func (a *CertCheckHttp) CheckCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	stime := time.Now()

	userInfo, err := a.checkCertificate(ctx, connState)

	etime := time.Now()
	dtime := etime.Sub(stime)
	dtimeSecs := float64(dtime) / float64(time.Second)

	strResult := ""
	if err != nil {
		if errors.Is(err, ErrInvalidAuth) {
			strResult = "bad_auth"
		} else {
			strResult = "error"
		}
	} else {
		strResult = "success"
	}

	certCheckLatencies.Record(ctx, dtimeSecs,
		metric.WithAttributes(
			attribute.String("server_address", a.hostPort),
			attribute.String("result", strResult),
		),
	)

	return userInfo, err
}
