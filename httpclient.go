package core

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type HTTPClient interface {
	Do(ctx context.Context, request *HTTPRequest) (*HTTPResponse, error)
	Reconfigure(config *HTTPClientConfig) error
	ManagementEndpoints() []string
}

// HTTPRequest contains the description of an HTTP request to perform.
type HTTPRequest struct {
	Service      ServiceType
	Method       string
	Endpoint     string
	Path         string
	Username     string
	Password     string
	Body         []byte
	Headers      map[string]string
	ContentType  string
	IsIdempotent bool
	UniqueID     string
}

// HTTPResponse encapsulates the response from an HTTP request.
type HTTPResponse struct {
	Raw *http.Response
}

type HTTPClientConfig struct {
	Username string
	Password string

	MgmtEndpoints   []string
	QueryEndpoints  []string
	SearchEndpoints []string
}

type HTTPClientOptions struct {
	Logger *zap.Logger

	TLSConfig           *tls.Config
	ConnectTimeout      time.Duration
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleTimeout         time.Duration
}

type httpClientState struct {
	username        string
	password        string
	mgmtEndpoints   []string
	queryEndpoints  []string
	searchEndpoints []string
}

type baseHTTPClient interface {
	Do(r *http.Request) (*http.Response, error)
}

type httpClient struct {
	cli    baseHTTPClient
	logger *zap.Logger

	state AtomicPointer[httpClientState]
}

func NewHTTPClient(config *HTTPClientConfig, opts *HTTPClientOptions) (*httpClient, error) {
	if config == nil {
		return nil, errors.New("must pass config for HttpClient")
	}

	if opts == nil {
		opts = &HTTPClientOptions{}
	}

	cli := &httpClient{
		logger: opts.Logger,
	}

	cli.state.Store(&httpClientState{})

	err := cli.Reconfigure(config)
	if err != nil {
		return nil, err
	}

	cli.setupBaseHTTPClient(opts)

	return cli, nil
}

func (c *httpClient) Do(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	state := c.state.Load()
	var endpoint string
	if req.Endpoint == "" {
		switch req.Service {
		case MgmtService:
			var err error
			endpoint, err = randFromServiceEndpoints(state.mgmtEndpoints)
			if err != nil {
				return nil, err
			}
		case QueryService:
			var err error
			req.Endpoint, err = randFromServiceEndpoints(state.queryEndpoints)
			if err != nil {
				return nil, err
			}
		case SearchService:
			var err error
			req.Endpoint, err = randFromServiceEndpoints(state.searchEndpoints)
			if err != nil {
				return nil, err
			}
		default:
			return nil, placeholderError{"unsupported service"}
		}
	} else {
		endpoint = req.Endpoint
	}

	header := make(http.Header)
	if req.ContentType == "" {
		header.Set("Content-Type", "application/json")
	} else {
		header.Set("Content-Type", req.ContentType)
	}

	for key, val := range req.Headers {
		header.Set(key, val)
	}
	var uniqueID string
	if req.UniqueID != "" {
		uniqueID = req.UniqueID
	} else {
		uniqueID = uuid.New().String()
	}
	header.Set("User-Agent", "core-http-agent-"+uniqueID)

	reqURI := endpoint + req.Path
	body := req.Body

	hreq, err := http.NewRequestWithContext(ctx, req.Method, reqURI, ioutil.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}

	if req.Username == "" && req.Password == "" {
		hreq.SetBasicAuth(state.username, state.password)
	} else {
		hreq.SetBasicAuth(req.Username, req.Password)
	}

	resp, err := c.cli.Do(hreq)
	if err != nil {
		return nil, err
	}

	return &HTTPResponse{Raw: resp}, nil
}

func (c *httpClient) Reconfigure(config *HTTPClientConfig) error {
	if config == nil {
		return errors.New("must pass config for reconfiguring http client")
	}

	oldState := c.state.Load()

	newState := &httpClientState{
		username: oldState.username,
		password: oldState.password,
	}

	if config.Username != "" {
		newState.username = config.Username
	}
	if config.Password != "" {
		newState.password = config.Password
	}

	newState.mgmtEndpoints = make([]string, len(config.MgmtEndpoints))
	copy(newState.mgmtEndpoints, config.MgmtEndpoints)
	newState.queryEndpoints = make([]string, len(config.QueryEndpoints))
	copy(newState.queryEndpoints, config.QueryEndpoints)
	newState.searchEndpoints = make([]string, len(config.SearchEndpoints))
	copy(newState.searchEndpoints, config.SearchEndpoints)

	c.state.Store(newState)

	return nil
}

func (c *httpClient) ManagementEndpoints() []string {
	state := c.state.Load()

	eps := make([]string, len(state.mgmtEndpoints))
	copy(eps, state.mgmtEndpoints)

	return eps
}

func (c *httpClient) setupBaseHTTPClient(opts *HTTPClientOptions) {
	httpDialer := &net.Dialer{
		Timeout:   opts.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}

	tlsConfig := opts.TLSConfig

	// We set ForceAttemptHTTP2, which will update the base-config to support HTTP2
	// automatically, so that all configs from it will look for that.
	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return httpDialer.DialContext(ctx, network, addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			tlsConn := tls.Client(tcpConn, tlsConfig)
			return tlsConn, nil
		},
		MaxIdleConns:        opts.MaxIdleConns,
		MaxIdleConnsPerHost: opts.MaxIdleConnsPerHost,
		IdleConnTimeout:     opts.IdleTimeout,
	}

	c.cli = &http.Client{
		Transport: httpTransport,
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

/* #nosec G404 */
func randFromServiceEndpoints(endpoints []string) (string, error) {
	if len(endpoints) == 0 {
		return "", placeholderError{"service not available"}
	}
	return endpoints[rand.Intn(len(endpoints))], nil
}
