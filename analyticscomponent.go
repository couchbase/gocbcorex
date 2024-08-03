package gocbcorex

import (
	"context"
	"net/http"

	"github.com/couchbase/gocbcorex/cbanalyticsx"
	"go.uber.org/zap"
)

type AnalyticsQueryOptions = cbanalyticsx.QueryOptions
type AnalyticsQueryResultStream = cbanalyticsx.QueryResultStream

type AnalyticsComponent struct {
	baseHttpComponent

	logger  *zap.Logger
	retries RetryManager
}

type AnalyticsComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        map[string]string
	Authenticator    Authenticator
}

type AnalyticsComponentOptions struct {
	Logger    *zap.Logger
	UserAgent string
}

func OrchestrateAnalyticsEndpoint[RespT any](
	ctx context.Context,
	w *AnalyticsComponent,
	fn func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error),
) (RespT, error) {
	roundTripper, _, endpoint, username, password, err := w.SelectEndpoint(nil)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	if endpoint == "" {
		var emptyResp RespT
		return emptyResp, serviceNotAvailableError{Service: ServiceTypeAnalytics}
	}

	return fn(roundTripper, endpoint, username, password)
}

func OrchestrateAnalyticsMgmtCall[OptsT any, RespT any](
	ctx context.Context,
	w *AnalyticsComponent,
	execFn func(o cbanalyticsx.Analytics, ctx context.Context, req OptsT) (RespT, error),
	opts OptsT,
) (RespT, error) {
	return OrchestrateRetries(ctx, w.retries, func() (RespT, error) {
		return OrchestrateAnalyticsEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error) {
				return execFn(cbanalyticsx.Analytics{
					Logger:    w.logger,
					UserAgent: w.userAgent,
					Transport: roundTripper,
					Endpoint:  endpoint,
					Username:  username,
					Password:  password,
				}, ctx, opts)
			})
	})
}

func OrchestrateNoResAnalyticsMgmtCall[OptsT any](
	ctx context.Context,
	w *AnalyticsComponent,
	execFn func(o cbanalyticsx.Analytics, ctx context.Context, req OptsT) error,
	opts OptsT,
) error {
	return OrchestrateNoResponseRetries(ctx, w.retries, func() error {
		_, err := OrchestrateAnalyticsEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (interface{}, error) {
				return nil, execFn(cbanalyticsx.Analytics{
					Logger:    w.logger,
					UserAgent: w.userAgent,
					Transport: roundTripper,
					Endpoint:  endpoint,
					Username:  username,
					Password:  password,
				}, ctx, opts)
			})
		return err
	})
}

func NewAnalyticsComponent(retries RetryManager, config *AnalyticsComponentConfig, opts *AnalyticsComponentOptions) *AnalyticsComponent {
	return &AnalyticsComponent{
		baseHttpComponent: baseHttpComponent{
			serviceType: ServiceTypeAnalytics,
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

func (w *AnalyticsComponent) Reconfigure(config *AnalyticsComponentConfig) error {
	w.updateState(baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	})
	return nil
}

type GetAnalyticsEndpointResult struct {
	RoundTripper http.RoundTripper
	Endpoint     string
	Username     string
	Password     string
}

func (w *AnalyticsComponent) GetEndpoint(ctx context.Context) (*GetAnalyticsEndpointResult, error) {
	return OrchestrateAnalyticsEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (*GetAnalyticsEndpointResult, error) {
			return &GetAnalyticsEndpointResult{
				RoundTripper: roundTripper,
				Endpoint:     endpoint,
				Username:     username,
				Password:     password,
			}, nil
		})
}

func (w *AnalyticsComponent) Query(ctx context.Context, opts *AnalyticsQueryOptions) (AnalyticsQueryResultStream, error) {
	return OrchestrateRetries(ctx, w.retries, func() (AnalyticsQueryResultStream, error) {
		return OrchestrateAnalyticsEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (AnalyticsQueryResultStream, error) {
				return cbanalyticsx.Analytics{
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
