package gocbcorex

import (
	"context"
	"net/http"

	"github.com/couchbase/gocbcorex/cbsearchx"

	"go.uber.org/zap"
)

type SearchComponent struct {
	baseHttpComponent

	logger  *zap.Logger
	retries RetryManager
}

type SearchComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type SearchComponentOptions struct {
	Logger    *zap.Logger
	UserAgent string
}

func OrchestrateSearchEndpoint[RespT any](
	ctx context.Context,
	w *SearchComponent,
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

func OrchestrateNoResSearchCall[OptsT any](
	ctx context.Context,
	w *SearchComponent,
	execFn func(o cbsearchx.Search, ctx context.Context, req OptsT) error,
	opts OptsT,
) error {
	_, err := OrchestrateSearchEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (interface{}, error) {
			return nil, execFn(cbsearchx.Search{
				UserAgent: w.userAgent,
				Transport: roundTripper,
				Endpoint:  endpoint,
				Username:  username,
				Password:  password,
			}, ctx, opts)
		})
	return err
}

func NewSearchComponent(retries RetryManager, config *SearchComponentConfig, opts *SearchComponentOptions) *SearchComponent {
	return &SearchComponent{
		baseHttpComponent: baseHttpComponent{
			serviceType: ServiceTypeSearch,
			userAgent:   opts.UserAgent,
			state: &baseHttpComponentState{
				httpRoundTripper: config.HttpRoundTripper,
				endpoints:        config.Endpoints,
				authenticator:    config.Authenticator,
			},
		},
		logger:  opts.Logger,
		retries: retries,
	}
}

func (w *SearchComponent) Reconfigure(config *SearchComponentConfig) error {
	w.lock.Lock()
	w.state = &baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	}
	w.lock.Unlock()
	return nil
}

func (w *SearchComponent) Query(ctx context.Context, opts *cbsearchx.QueryOptions) (cbsearchx.QueryResultStream, error) {
	return OrchestrateSearchRetries(ctx, w.retries, func() (cbsearchx.QueryResultStream, error) {
		return OrchestrateSearchEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (cbsearchx.QueryResultStream, error) {
				return cbsearchx.Search{
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

func (w *SearchComponent) UpsertIndex(ctx context.Context, opts *cbsearchx.UpsertIndexOptions) error {
	return OrchestrateNoResSearchCall(ctx, w, cbsearchx.Search.UpsertIndex, opts)
}

func (w *SearchComponent) DeleteIndex(ctx context.Context, opts *cbsearchx.DeleteIndexOptions) error {
	return OrchestrateNoResSearchCall(ctx, w, cbsearchx.Search.DeleteIndex, opts)
}
