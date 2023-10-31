package gocbcorex

import (
	"context"
	"net/http"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

type QueryOptions = cbqueryx.Options
type QueryResultStream = cbqueryx.ResultStream
type PreparedStatementCache = cbqueryx.PreparedStatementCache

type QueryComponent struct {
	baseHttpComponent

	logger        *zap.Logger
	retries       RetryManager
	preparedCache *PreparedStatementCache
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

func OrchestrateQueryEndpoint[RespT any](
	ctx context.Context,
	w *QueryComponent,
	fn func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error),
) (RespT, error) {
	roundTripper, endpoint, username, password, err := w.SelectEndpoint(nil)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	if endpoint == "" {
		var emptyResp RespT
		return emptyResp, ErrServiceNotAvailable
	}

	return fn(roundTripper, endpoint, username, password)
}

func NewQueryComponent(retries RetryManager, config *QueryComponentConfig, opts *QueryComponentOptions) *QueryComponent {
	return &QueryComponent{
		baseHttpComponent: baseHttpComponent{
			serviceType: ServiceTypeQuery,
			userAgent:   opts.UserAgent,
			state: &baseHttpComponentState{
				httpRoundTripper: config.HttpRoundTripper,
				endpoints:        config.Endpoints,
				authenticator:    config.Authenticator,
			},
		},
		logger:        opts.Logger,
		retries:       retries,
		preparedCache: cbqueryx.NewPreparedStatementCache(),
	}
}

func (w *QueryComponent) Reconfigure(config *QueryComponentConfig) error {
	w.lock.Lock()
	w.state = &baseHttpComponentState{
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
