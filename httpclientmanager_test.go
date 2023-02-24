package gocbcorex

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type configStreamBlock struct {
	Bytes []byte
}

func (i *configStreamBlock) UnmarshalJSON(data []byte) error {
	i.Bytes = make([]byte, len(data))
	copy(i.Bytes, data)
	return nil
}

func TestHTTPClientManagerNewAppliesFirstConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	mgmtEndpoints := []string{"endpoint1", "endpoint2"}
	expectedClient := &HTTPClientMock{}
	cfg := &HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator",
				Password: "password",
			},
			MgmtEndpoints: mgmtEndpoints,
		},
	}
	connectTimeout := 1 * time.Second
	maxIdle := 1
	maxIdlePerHost := 2
	idleTimeout := 2 * time.Minute

	mgr, err := NewHTTPClientManager(cfg, &HTTPClientManagerOptions{
		Logger: logger,
		NewHTTPClientFn: func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
			assert.Equal(t, &cfg.HTTPClientConfig, clientConfig)
			assert.Equal(t, connectTimeout, clientOpts.ConnectTimeout)
			assert.Equal(t, maxIdle, clientOpts.MaxIdleConns)
			assert.Equal(t, maxIdlePerHost, clientOpts.MaxIdleConnsPerHost)
			assert.Equal(t, idleTimeout, clientOpts.IdleTimeout)
			assert.Nil(t, clientOpts.TLSConfig)

			return expectedClient, nil
		},
		ConnectTimeout:      connectTimeout,
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdlePerHost,
		IdleTimeout:         idleTimeout,
	})
	require.NoError(t, err)

	cli, err := mgr.GetClient()
	require.NoError(t, err)

	assert.Equal(t, expectedClient, cli)
}

func TestHTTPClientManagerNewErrorFromNewHTTPClient(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	mgmtEndpoints := []string{"endpoint1", "endpoint2"}
	expectedError := errors.New("some error")

	_, err := NewHTTPClientManager(&HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator",
				Password: "password",
			},
			MgmtEndpoints: mgmtEndpoints,
		},
	}, &HTTPClientManagerOptions{
		Logger: logger,
		NewHTTPClientFn: func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
			return nil, expectedError
		},
	})
	require.ErrorIs(t, err, expectedError)
}

func TestHTTPClientManagerNewNilConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	_, err := NewHTTPClientManager(nil, &HTTPClientManagerOptions{
		Logger: logger,
		NewHTTPClientFn: func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
			t.FailNow()
			return nil, errors.New("shouldnt have reached here")
		},
	})
	require.Error(t, err)
}

func TestHTTPClientManagerReconfigureNilConfig(t *testing.T) {
	mgr := &httpClientManager{}
	err := mgr.Reconfigure(nil)
	require.Error(t, err)
}

func TestHTTPClientManagerReconfigureSameTLSConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	var reconfigures int
	mgmtEndpoints := []string{"endpoint1", "endpoint2"}
	cfg := &HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator",
				Password: "password",
			},
			MgmtEndpoints: mgmtEndpoints,
		},
		TLSConfig: nil,
	}
	newCfg := &HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator2",
				Password: "password2",
			},
			MgmtEndpoints: append(mgmtEndpoints, "endpoint3"),
		},
		TLSConfig: nil,
	}
	expectedClient := &HTTPClientMock{
		ReconfigureFunc: func(config *HTTPClientConfig) error {
			assert.Equal(t, &newCfg.HTTPClientConfig, config)
			reconfigures++

			return nil
		},
	}
	connectTimeout := 1 * time.Second
	maxIdle := 1
	maxIdlePerHost := 2
	idleTimeout := 2 * time.Minute

	mgr, err := NewHTTPClientManager(cfg, &HTTPClientManagerOptions{
		Logger: logger,
		NewHTTPClientFn: func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
			assert.Equal(t, &cfg.HTTPClientConfig, clientConfig)
			assert.Equal(t, connectTimeout, clientOpts.ConnectTimeout)
			assert.Equal(t, maxIdle, clientOpts.MaxIdleConns)
			assert.Equal(t, maxIdlePerHost, clientOpts.MaxIdleConnsPerHost)
			assert.Equal(t, idleTimeout, clientOpts.IdleTimeout)
			assert.Nil(t, clientOpts.TLSConfig)

			return expectedClient, nil
		},
		ConnectTimeout:      connectTimeout,
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdlePerHost,
		IdleTimeout:         idleTimeout,
	})
	require.NoError(t, err)

	err = mgr.Reconfigure(newCfg)
	require.NoError(t, err)

	assert.Equal(t, 1, reconfigures)
}

func TestHTTPClientManagerReconfigureDifferentTLSConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	mgmtEndpoints := []string{"endpoint1", "endpoint2"}
	cfg := &HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator",
				Password: "password",
			},
			MgmtEndpoints: mgmtEndpoints,
		},
		TLSConfig: nil,
	}
	newTlsCfg := &tls.Config{}
	newCfg := &HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: "Administrator2",
				Password: "password2",
			},
			MgmtEndpoints: append(mgmtEndpoints, "endpoint3"),
		},
		TLSConfig: newTlsCfg,
	}
	var numCloses int
	expectedClient := &HTTPClientMock{
		CloseFunc: func() error {
			numCloses++
			return nil
		},
	}
	connectTimeout := 1 * time.Second
	maxIdle := 1
	maxIdlePerHost := 2
	idleTimeout := 2 * time.Minute

	var numNewCalls int
	mgr, err := NewHTTPClientManager(cfg, &HTTPClientManagerOptions{
		Logger: logger,
		NewHTTPClientFn: func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
			numNewCalls++
			if numNewCalls == 1 {
				assert.Equal(t, &cfg.HTTPClientConfig, clientConfig)
				assert.Nil(t, clientOpts.TLSConfig)
			} else {
				assert.Equal(t, &newCfg.HTTPClientConfig, clientConfig)
				assert.Equal(t, newTlsCfg, clientOpts.TLSConfig)
			}

			return expectedClient, nil
		},
		ConnectTimeout:      connectTimeout,
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdlePerHost,
		IdleTimeout:         idleTimeout,
	})
	require.NoError(t, err)

	err = mgr.Reconfigure(newCfg)
	require.NoError(t, err)

	assert.Equal(t, 1, numCloses)
	assert.Equal(t, 2, numNewCalls)
}

// TODO(chvck): I'm not actually sure what this is testing, really we need a way to test that the client transport
//
//	is closed after we close the body.
func TestHTTPClientManagerReconfigureWhileStreaming(t *testing.T) {
	testutils.SkipIfShortTest(t)

	logger, _ := zap.NewDevelopment()

	mgmtEndpoints := make([]string, len(testutils.TestOpts.HTTPAddrs))
	for i, ep := range testutils.TestOpts.HTTPAddrs {
		mgmtEndpoints[i] = "http://" + ep
	}

	mgr, err := NewHTTPClientManager(&HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: testutils.TestOpts.Username,
				Password: testutils.TestOpts.Password,
			},
			MgmtEndpoints: mgmtEndpoints,
		},
	}, &HTTPClientManagerOptions{
		Logger:              logger,
		NewHTTPClientFn:     nil,
		ConnectTimeout:      0,
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: 0,
		IdleTimeout:         0,
	})
	require.NoError(t, err)

	resp := sendHTTPRequest(t, mgr, &HTTPRequest{
		Service: MgmtService,
		Method:  "GET",
		Path:    "/pools/default/bs/" + testutils.TestOpts.BucketName,
	})
	require.Equal(t, 200, resp.Raw.StatusCode)

	err = mgr.Reconfigure(&HTTPClientManagerConfig{
		HTTPClientConfig: HTTPClientConfig{
			Authenticator: &PasswordAuthenticator{
				Username: testutils.TestOpts.Username,
				Password: testutils.TestOpts.Password,
			},
			MgmtEndpoints: testutils.TestOpts.HTTPAddrs,
		},
		TLSConfig: &tls.Config{},
	})
	require.NoError(t, err)

	// Check that the stream is still alive after reconfigure
	dec := json.NewDecoder(resp.Raw.Body)
	configBlock := new(configStreamBlock)

	err = dec.Decode(configBlock)
	require.NoError(t, err)

	err = resp.Raw.Body.Close()
	require.NoError(t, err)
}

func sendHTTPRequest(t *testing.T, mgr *httpClientManager, req *HTTPRequest) *HTTPResponse {
	cli, err := mgr.GetClient()
	require.NoError(t, err)

	resp, err := cli.Do(context.Background(), req)
	require.NoError(t, err)

	return resp
}
