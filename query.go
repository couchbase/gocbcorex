package gocbcorex

import (
	"context"
	"math/rand"
	"net/http"
	"sync"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

type QueryOptions = cbqueryx.QueryOptions
type QueryResultStream = cbqueryx.QueryResultStream
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

func (w *QueryComponent) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (QueryResultStream, error) {
		w.lock.RLock()
		state := *w.state
		w.lock.RUnlock()

		endpoint, err := randFromServiceEndpoints(state.endpoints, nil)
		if err != nil {
			return nil, err
		}

		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, err
		}

		username, password, err := state.authenticator.GetCredentials(QueryService, host)
		if err != nil {
			return nil, err
		}

		return cbqueryx.Query{
			Logger:    w.logger,
			Transport: state.httpRoundTripper,
			UserAgent: state.userAgent,
			Endpoint:  endpoint,
			Username:  username,
			Password:  password,
		}.Query(ctx, opts)
	})
}

func (w *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (QueryResultStream, error) {
		w.lock.RLock()
		state := *w.state
		w.lock.RUnlock()

		endpoint, err := randFromServiceEndpoints(state.endpoints, nil)
		if err != nil {
			return nil, err
		}

		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, err
		}

		username, password, err := state.authenticator.GetCredentials(QueryService, host)
		if err != nil {
			return nil, err
		}

		return cbqueryx.PreparedQuery{
			Executor: cbqueryx.Query{
				Logger:    w.logger,
				Transport: state.httpRoundTripper,
				UserAgent: state.userAgent,
				Endpoint:  endpoint,
				Username:  username,
				Password:  password,
			},
			Cache: w.preparedCache,
		}.PreparedQuery(ctx, opts)
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
