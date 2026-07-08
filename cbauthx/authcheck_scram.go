package cbauthx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type ScramAuthValidatorOptions struct {
	Transport   http.RoundTripper
	Uri         string
	ClusterUuid string
	Mechanism   string
}

type ScramAuthValidator struct {
	transport   http.RoundTripper
	uri         string
	clusterUuid string
	mechanism   string
	sid         string
}

func NewScramAuthValidator(
	opts *ScramAuthValidatorOptions,
) (*ScramAuthValidator, error) {
	if opts.Uri == "" {
		return nil, errors.New("uri is required")
	}

	mech := opts.Mechanism
	if mech == "" {
		mech = "SCRAM-SHA-256"
	}

	if mech != "SCRAM-SHA-1" &&
		mech != "SCRAM-SHA-256" &&
		mech != "SCRAM-SHA-512" {
		return nil, fmt.Errorf("unsupported SCRAM mechanism: %s", mech)
	}

	transport := opts.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	return &ScramAuthValidator{
		transport:   transport,
		uri:         opts.Uri,
		clusterUuid: opts.ClusterUuid,
		mechanism:   mech,
	}, nil
}

func (v *ScramAuthValidator) Step1(
	ctx context.Context,
	clientFirstMsg []byte,
) ([]byte, error) {
	clientFirstMsgB64 := base64.StdEncoding.EncodeToString(clientFirstMsg)
	authHeaderVal := fmt.Sprintf("%s data=%s", v.mechanism, clientFirstMsgB64)

	req, err := http.NewRequestWithContext(ctx, "POST", v.uri, nil)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to create step 1 request",
			Cause:   err,
		}
	}

	req.Header.Set("Authorization", authHeaderVal)

	resp, err := v.transport.RoundTrip(req)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to execute step 1 auth request",
			Cause:   err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"expected 401 status code in step 1, got %d: %s",
			resp.StatusCode,
			respBody,
		)
	}

	wwwAuth := resp.Header.Get("WWW-Authenticate")
	if wwwAuth == "" {
		return nil, errors.New(
			"missing WWW-Authenticate header in step 1 response",
		)
	}

	prefix := v.mechanism + " "
	if !strings.HasPrefix(wwwAuth, prefix) {
		return nil, fmt.Errorf(
			"unexpected WWW-Authenticate header prefix: %s",
			wwwAuth,
		)
	}

	challengeBody := strings.TrimPrefix(wwwAuth, prefix)
	var sid, dataB64 string
	parts := strings.Split(challengeBody, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "sid=") {
			sid = strings.TrimPrefix(part, "sid=")
		} else if strings.HasPrefix(part, "data=") {
			dataB64 = strings.TrimPrefix(part, "data=")
		}
	}

	if sid == "" {
		return nil, errors.New("missing sid in WWW-Authenticate header")
	}
	if dataB64 == "" {
		return nil, errors.New("missing data in WWW-Authenticate header")
	}

	serverFirstMsg, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to decode server first message: %w",
			err,
		)
	}

	v.sid = sid
	return serverFirstMsg, nil
}

func (v *ScramAuthValidator) Step2(
	ctx context.Context,
	clientFinalMsg []byte,
) ([]byte, UserInfo, error) {
	if v.sid == "" {
		return nil, UserInfo{}, errors.New(
			"cannot call Step2 before Step1 succeeds",
		)
	}

	clientFinalMsgB64 := base64.StdEncoding.EncodeToString(clientFinalMsg)
	authHeaderVal := fmt.Sprintf(
		"%s data=%s,sid=%s",
		v.mechanism,
		clientFinalMsgB64,
		v.sid,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", v.uri, nil)
	if err != nil {
		return nil, UserInfo{}, &contextualError{
			Message: "failed to create step 2 request",
			Cause:   err,
		}
	}

	req.Header.Set("Authorization", authHeaderVal)

	resp, err := v.transport.RoundTrip(req)
	if err != nil {
		return nil, UserInfo{}, &contextualError{
			Message: "failed to execute step 2 auth request",
			Cause:   err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, UserInfo{}, ErrInvalidAuth
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, UserInfo{}, fmt.Errorf(
			"received non-200/401 status code in step 2: %d: %s",
			resp.StatusCode,
			respBody,
		)
	}

	authInfo := resp.Header.Get("Authentication-Info")
	if authInfo == "" {
		return nil, UserInfo{}, errors.New(
			"missing Authentication-Info header in step 2 response",
		)
	}

	var respSid, respDataB64 string
	parts := strings.Split(authInfo, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "sid=") {
			respSid = strings.TrimPrefix(part, "sid=")
		} else if strings.HasPrefix(part, "data=") {
			respDataB64 = strings.TrimPrefix(part, "data=")
		}
	}

	if respSid != "" && respSid != v.sid {
		return nil, UserInfo{}, fmt.Errorf(
			"Authentication-Info sid %s does not match session %s",
			respSid,
			v.sid,
		)
	}

	if respDataB64 == "" {
		return nil, UserInfo{}, errors.New(
			"missing data in Authentication-Info header",
		)
	}

	serverFinalMsg, err := base64.StdEncoding.DecodeString(respDataB64)
	if err != nil {
		return nil, UserInfo{}, fmt.Errorf(
			"failed to decode server final message: %w",
			err,
		)
	}

	var jsonResp AuthCheckResponse
	err = json.NewDecoder(resp.Body).Decode(&jsonResp)
	if err != nil {
		return nil, UserInfo{}, &contextualError{
			Message: "failed to decode response data",
			Cause:   err,
		}
	}

	if jsonResp.Domain == "" {
		return nil, UserInfo{}, errors.New(
			"domain field missing from auth response",
		)
	}

	return serverFinalMsg, UserInfo{
		Domain: jsonResp.Domain,
		Uuid:   jsonResp.Uuid,
	}, nil
}
