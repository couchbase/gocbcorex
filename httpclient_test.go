package gocbcorex

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientNewAppliesConfig(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2"},
		SearchEndpoints: nil,
	}
	clientMock := &BaseHTTPClientMock{}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}
	var numNewCalls int
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		numNewCalls++

		assert.Equal(t, opts.TLSConfig, clientOpts.TLSConfig)
		assert.Equal(t, opts.ConnectTimeout, clientOpts.ConnectTimeout)
		assert.Equal(t, opts.MaxIdleConns, clientOpts.MaxIdleConns)
		assert.Equal(t, opts.MaxIdleConnsPerHost, clientOpts.MaxIdleConnsPerHost)
		assert.Equal(t, opts.IdleTimeout, clientOpts.IdleTimeout)

		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	assertHTTPClientState(t, cli, cfg)

	assert.Equal(t, 1, numNewCalls)
}

func TestHTTPClientNewNilConfig(t *testing.T) {
	_, err := NewHTTPClient(nil, &HTTPClientOptions{})
	require.Error(t, err)
}

func TestHTTPClientNewErrorFromNewBaseClient(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2"},
		SearchEndpoints: nil,
	}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}

	expectedError := errors.New("amerror")
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		return nil, expectedError
	}

	_, err := NewHTTPClient(cfg, opts)
	require.Error(t, err)
}

func TestHTTPClientReconfigureAppliesConfig(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2"},
		SearchEndpoints: nil,
	}
	newCfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2", "mgmt3"},
		SearchEndpoints: []string{"search1"},
	}
	clientMock := &BaseHTTPClientMock{}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}
	var numNewCalls int
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		numNewCalls++

		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	err = cli.Reconfigure(newCfg)
	require.NoError(t, err)

	assertHTTPClientState(t, cli, newCfg)

	assert.Equal(t, 1, numNewCalls)
}

func TestHTTPClientReconfigureAppliesConfigWithoutCreds(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2"},
		SearchEndpoints: nil,
	}
	newCfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"mgmt1", "mgmt2", "mgmt3"},
		SearchEndpoints: []string{"search1"},
	}
	clientMock := &BaseHTTPClientMock{}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}
	var numNewCalls int
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		numNewCalls++

		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	err = cli.Reconfigure(newCfg)
	require.NoError(t, err)

	assertHTTPClientState(t, cli, &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   newCfg.MgmtEndpoints,
		SearchEndpoints: newCfg.SearchEndpoints,
	})

	assert.Equal(t, 1, numNewCalls)
}

func TestHTTPClientDo(t *testing.T) {
	auth := &PasswordAuthenticator{
		Username: "Administrator",
		Password: "password",
	}
	cfg := &HTTPClientConfig{
		Authenticator:   auth,
		MgmtEndpoints:   []string{"http://mgmt1", "http://mgmt2"},
		SearchEndpoints: nil,
	}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}

	ctx := context.Background()
	req := &HTTPRequest{
		Service:      MgmtService,
		Method:       "POST",
		Endpoint:     "",
		Path:         "/pools/default/nodesServices",
		Username:     "",
		Password:     "",
		Body:         []byte("somnething"),
		Headers:      map[string]string{"test": "cheddar"},
		ContentType:  "",
		IsIdempotent: false,
		UniqueID:     "",
	}
	expectedResp := &http.Response{}

	clientMock := &BaseHTTPClientMock{
		DoFunc: func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, ctx, r.Context())
			assert.Equal(t, req.Method, r.Method)
			username, password, ok := r.BasicAuth()
			if assert.True(t, ok) {
				assert.Equal(t, auth.Username, username)
				assert.Equal(t, auth.Password, password)
			}
			assert.Equal(t, req.Path, r.URL.Path)
			assert.Contains(t, cfg.MgmtEndpoints, "http://"+r.URL.Host)
			body := make([]byte, len(req.Body))
			_, err := r.Body.Read(body)
			if assert.NoError(t, err) {
				assert.Equal(t, req.Body, body)
			}
			assert.Equal(t, req.Headers["test"], r.Header.Get("test"))

			// Things that should have been auto-populated by our client.
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.NotEmpty(t, r.UserAgent())

			return expectedResp, nil
		},
	}
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	resp, err := cli.Do(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, expectedResp, resp.Raw)
}

func TestHTTPClientDoPassthroughNonDefaults(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"http://mgmt1", "http://mgmt2"},
		SearchEndpoints: nil,
	}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}

	ctx := context.Background()
	req := &HTTPRequest{
		Service:     MgmtService,
		Method:      "POST",
		Endpoint:    "http://mgmt3",
		Path:        "/pools/default/nodesServices",
		Username:    "Barry",
		Password:    "barryssecret",
		Body:        nil,
		Headers:     map[string]string{"test": "cheddar"},
		ContentType: "application/barrysapplication",
		UniqueID:    "1234",
	}
	expectedResp := &http.Response{}

	clientMock := &BaseHTTPClientMock{
		DoFunc: func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, ctx, r.Context())
			assert.Equal(t, req.Method, r.Method)
			username, password, ok := r.BasicAuth()
			if assert.True(t, ok) {
				assert.Equal(t, req.Username, username)
				assert.Equal(t, req.Password, password)
			}
			assert.Equal(t, req.Path, r.URL.Path)
			assert.Equal(t, req.Endpoint, "http://"+r.URL.Host)
			assert.Equal(t, req.Headers["test"], r.Header.Get("test"))

			// Things that should have been auto-populated by our client.
			assert.Equal(t, req.ContentType, r.Header.Get("Content-Type"))
			assert.Equal(t, "core-http-agent-"+req.UniqueID, r.UserAgent())

			return expectedResp, nil
		},
	}
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	resp, err := cli.Do(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, expectedResp, resp.Raw)
}

func TestHTTPClientDoUnsupportedService(t *testing.T) {
	cfg := &HTTPClientConfig{
		Authenticator: &PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		MgmtEndpoints:   []string{"http://mgmt1", "http://mgmt2"},
		SearchEndpoints: nil,
	}
	opts := &HTTPClientOptions{
		TLSConfig:           nil,
		ConnectTimeout:      1 * time.Second,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 3,
		IdleTimeout:         1 * time.Minute,
	}

	ctx := context.Background()
	req := &HTTPRequest{
		Service:     ServiceType(99),
		Method:      "POST",
		Endpoint:    "",
		Path:        "/pools/default/nodesServices",
		Username:    "Barry",
		Password:    "barryssecret",
		Body:        nil,
		Headers:     map[string]string{"test": "cheddar"},
		ContentType: "application/barrysapplication",
		UniqueID:    "1234",
	}

	clientMock := &BaseHTTPClientMock{}
	opts.NewClientFunc = func(clientOpts *HTTPClientOptions) (BaseHTTPClient, error) {
		return clientMock, nil
	}

	cli, err := NewHTTPClient(cfg, opts)
	require.NoError(t, err)

	_, err = cli.Do(ctx, req)
	require.Error(t, err)
}

func assertHTTPClientState(t *testing.T, cli *httpClient, cfg *HTTPClientConfig) {
	// We shouldn't really inspect internal state like this, but it's the only real way to test this
	// without exposing a bunch of functions that we don't want to.
	state := cli.state.Load()

	assert.Equal(t, cfg.Authenticator, state.authenticator)
	assert.Equal(t, cfg.MgmtEndpoints, state.mgmtEndpoints)
	assert.Equal(t, cfg.SearchEndpoints, state.searchEndpoints)
}
