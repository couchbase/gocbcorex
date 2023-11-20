package gocbcorex

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"

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
		return emptyResp, serviceNotAvailableError{Service: ServiceTypeQuery}
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
	w.updateState(baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	})
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

type EnsureQueryIndexCreatedOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (w *QueryComponent) EnsureIndexCreated(ctx context.Context, opts *EnsureQueryIndexCreatedOptions) error {
	hlpr := cbqueryx.EnsureIndexHelper{
		Logger:         w.logger.Named("ensure-index"),
		UserAgent:      w.userAgent,
		OnBehalfOf:     opts.OnBehalfOf,
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		IndexName:      opts.IndexName,
	}

	backoff := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, backoff, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.PollCreated(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToQueryx(),
		})
	})
}

type EnsureQueryIndexDroppedOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (w *QueryComponent) EnsureIndexDropped(ctx context.Context, opts *EnsureQueryIndexDroppedOptions) error {
	hlpr := cbqueryx.EnsureIndexHelper{
		Logger:         w.logger.Named("ensure-index"),
		UserAgent:      w.userAgent,
		OnBehalfOf:     opts.OnBehalfOf,
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		IndexName:      opts.IndexName,
	}

	backoff := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, backoff, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.PollDropped(ctx, &cbqueryx.EnsureIndexPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToQueryx(),
		})
	})
}
