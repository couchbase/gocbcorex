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
	userAgent     string

	lock  sync.RWMutex
	state *queryComponentState
}

type QueryComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type QueryComponentOptions struct {
	Logger    *zap.Logger
	UserAgent string
}

type queryComponentState struct {
	httpRoundTripper http.RoundTripper
	endpoints        []string
	authenticator    Authenticator
}

func OrchestrateQueryEndpoint[RespT any](
	ctx context.Context,
	w *QueryComponent,
	fn func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error),
) (RespT, error) {
	var recentEndpoints []string

	for {
		w.lock.RLock()
		state := *w.state
		w.lock.RUnlock()

		// if there are no endpoints to query, we can't proceed
		if len(state.endpoints) == 0 {
			var emptyResp RespT
			return emptyResp, ErrServiceNotAvailable
		}

		// remove all the endpoints we've already tried
		remainingEndpoints := filterStringsOut(state.endpoints, recentEndpoints)

		// if there are no more endpoints to try, we can't proceed
		if len(remainingEndpoints) == 0 {
			// TODO(brett19): Decide if this is the right error to return...
			var emptyResp RespT
			return emptyResp, ErrServiceNotAvailable
		}

		// pick a random endpoint to attempt
		endpoint := remainingEndpoints[rand.Intn(len(remainingEndpoints))]

		// mark the selected endpoint as having been tried
		recentEndpoints = append(recentEndpoints, endpoint)

		host, err := getHostFromUri(endpoint)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		username, password, err := state.authenticator.GetCredentials(QueryService, host)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		res, err := fn(state.httpRoundTripper, endpoint, username, password)
		if err != nil {
			// TODO(brett19): Handle certain kinds of errors that mean sending to a different node...
			if false {
				// certain errors loop back around to try again with a different endpoint
				continue
			}

			var emptyResp RespT
			return emptyResp, err
		}

		return res, nil
	}
}

func NewQueryComponent(retries RetryManager, config *QueryComponentConfig, opts *QueryComponentOptions) *QueryComponent {
	return &QueryComponent{
		logger:    opts.Logger,
		userAgent: opts.UserAgent,
		retries:   retries,
		state: &queryComponentState{
			httpRoundTripper: config.HttpRoundTripper,
			endpoints:        config.Endpoints,
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
		authenticator:    config.Authenticator,
	}
	w.lock.Unlock()
	return nil
}

func (w *QueryComponent) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (QueryResultStream, error) {
		return OrchestrateQueryEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (QueryResultStream, error) {
				return cbqueryx.Query{
					Logger:    w.logger,
					UserAgent: w.userAgent,
					Transport: roundTripper,
					Endpoint:  endpoint,
					Username:  username,
					Password:  password,
				}.Query(ctx, opts)
			})
	})
}

func (w *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateQueryRetries(ctx, w.retries, func() (QueryResultStream, error) {
		return OrchestrateQueryEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (QueryResultStream, error) {
				return cbqueryx.PreparedQuery{
					Executor: cbqueryx.Query{
						Logger:    w.logger,
						UserAgent: w.userAgent,
						Transport: roundTripper,
						Endpoint:  endpoint,
						Username:  username,
						Password:  password,
					},
					Cache: w.preparedCache,
				}.PreparedQuery(ctx, opts)
			})
	})
}
