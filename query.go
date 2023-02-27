package gocbcorex

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"sync"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

func OrchestrateQuery[RespT any](ctx context.Context, fn func() (RespT, error)) (RespT, error) {
	res, err := fn()
	if err != nil {
		// TODO(brett19): Possibly retry some errors
		return res, err
	}

	return res, nil
}

// makeHTTPClient creates a client with the specified round tripper and CheckRedirect set so that
// retries contain auth details.
func makeHTTPClient(roundTripper http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: roundTripper,
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

type QueryOptions = cbqueryx.QueryOptions
type QueryRowReader = cbqueryx.QueryRowReader
type PreparedStatementCache = cbqueryx.PreparedStatementCache

type QueryComponent struct {
	logger        *zap.Logger
	retries       RetryManager
	preparedCache *PreparedStatementCache

	lock  sync.RWMutex
	state *queryComponentState
}

type QueryComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
}

type QueryComponentOptions struct {
	Logger *zap.Logger
}

type queryComponentState struct {
	httpRoundTripper http.RoundTripper
	endpoints        []string
	userAgent        string
	authenticator    Authenticator
}

func NewQueryComponent(retries RetryManager, config *QueryComponentConfig, opts *QueryComponentOptions) *QueryComponent {
	return &QueryComponent{
		logger:  opts.Logger,
		retries: retries,
		state: &queryComponentState{
			httpRoundTripper: config.HttpRoundTripper,
			endpoints:        config.Endpoints,
			userAgent:        config.UserAgent,
			authenticator:    config.Authenticator,
		},
		preparedCache: cbqueryx.NewPreparedStatementCache(),
	}
}

func (w *QueryComponent) Reconfigure(config *QueryComponentConfig) error {
	w.lock.Lock()
	w.state = &queryComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		userAgent:        config.UserAgent,
		authenticator:    config.Authenticator,
	}
	w.lock.Unlock()
	return nil
}

func (w *QueryComponent) Query(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (*QueryRowReader, error) {
		w.lock.RLock()
		state := *w.state
		w.lock.RUnlock()
		endpoint := opts.Endpoint
		if endpoint == "" {
			var err error
			endpoint, err = randFromServiceEndpoints(state.endpoints, nil)
			if err != nil {
				return nil, err
			}

			opts.Endpoint = endpoint // TODO(chvck): this isn't right, it'll alter the options we've given.
		}
		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, err
		}

		username, password, err := state.authenticator.GetCredentials(QueryService, host)
		if err != nil {
			return nil, err
		}

		httpClient := makeHTTPClient(state.httpRoundTripper)
		return OrchestrateQuery(ctx, func() (*cbqueryx.QueryRowReader, error) {
			return cbqueryx.Query{
				HttpClient: httpClient,
				Logger:     w.logger,
				QueryCache: nil,
				UserAgent:  state.userAgent,
				Username:   username,
				Password:   password,
			}.Query(ctx, opts)
		})
	})
}

func (w *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (*QueryRowReader, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (*QueryRowReader, error) {
		w.lock.RLock()
		state := *w.state
		w.lock.RUnlock()
		endpoint := opts.Endpoint
		if endpoint == "" {
			var err error
			endpoint, err = randFromServiceEndpoints(state.endpoints, nil)
			if err != nil {
				return nil, err
			}

			opts.Endpoint = endpoint // TODO(chvck): this isn't right, it'll alter the options we've given.
		}
		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, err
		}

		username, password, err := state.authenticator.GetCredentials(QueryService, host)
		if err != nil {
			return nil, err
		}

		httpClient := makeHTTPClient(state.httpRoundTripper)
		return OrchestrateQuery(ctx, func() (*cbqueryx.QueryRowReader, error) {
			return cbqueryx.Query{
				HttpClient: httpClient,
				Logger:     w.logger,
				QueryCache: w.preparedCache,
				UserAgent:  state.userAgent,
				Username:   username,
				Password:   password,
			}.PreparedQuery(ctx, opts)
		})
	})
}

/* #nosec G404 */
func randFromServiceEndpoints(endpoints []string, denylist []string) (string, error) {
	var allowList []string
	for _, ep := range endpoints {
		if inDenyList(ep, denylist) {
			continue
		}
		allowList = append(allowList, ep)
	}
	if len(allowList) == 0 {
		return "", ErrServiceNotAvailable
	}

	return allowList[rand.Intn(len(allowList))], nil
}

func inDenyList(ep string, denylist []string) bool {
	for _, b := range denylist {
		if ep == b {
			return true
		}
	}

	return false
}
