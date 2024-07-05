package gocbcorex

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"

	"github.com/couchbase/gocbcorex/cbqueryx"
	"go.uber.org/zap"
)

type QueryOptions struct {
	cbqueryx.QueryOptions
	Endpoint string
}

type QueryResultStream interface {
	cbqueryx.ResultStream
	Endpoint() string
}

type PreparedStatementCache = cbqueryx.PreparedStatementCache

type QueryComponent struct {
	baseHttpComponent

	logger        *zap.Logger
	retries       RetryManager
	preparedCache *PreparedStatementCache
}

type QueryComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        map[string]string
	Authenticator    Authenticator
}

type QueryComponentOptions struct {
	Logger    *zap.Logger
	UserAgent string
}

func OrchestrateQueryEndpoint[RespT any](
	ctx context.Context,
	w *QueryComponent,
	endpointId string,
	fn func(roundTripper http.RoundTripper, endpointId, endpoint, username, password string) (RespT, error),
) (RespT, error) {
	if endpointId != "" {
		roundTripper, endpoint, username, password, err := w.SelectSpecificEndpoint(endpointId)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		return fn(roundTripper, endpointId, endpoint, username, password)
	}

	roundTripper, endpointId, endpoint, username, password, err := w.SelectEndpoint(nil)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	if endpoint == "" {
		var emptyResp RespT
		return emptyResp, serviceNotAvailableError{Service: ServiceTypeQuery}
	}

	return fn(roundTripper, endpointId, endpoint, username, password)
}

func OrchestrateQueryMgmtCall[OptsT any, RespT any](
	ctx context.Context,
	w *QueryComponent,
	execFn func(o cbqueryx.Query, ctx context.Context, req OptsT) (RespT, error),
	opts OptsT,
) (RespT, error) {
	return OrchestrateRetries(ctx, w.retries, func() (RespT, error) {
		return OrchestrateQueryEndpoint(ctx, w, "",
			func(roundTripper http.RoundTripper, _, endpoint, username, password string) (RespT, error) {
				return execFn(cbqueryx.Query{
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

func OrchestrateNoResQueryMgmtCall[OptsT any](
	ctx context.Context,
	w *QueryComponent,
	execFn func(o cbqueryx.Query, ctx context.Context, req OptsT) error,
	opts OptsT,
) error {
	return OrchestrateNoResponseRetries(ctx, w.retries, func() error {
		_, err := OrchestrateQueryEndpoint(ctx, w, "",
			func(roundTripper http.RoundTripper, _, endpoint, username, password string) (interface{}, error) {
				return nil, execFn(cbqueryx.Query{
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

type queryResultStream struct {
	cbqueryx.ResultStream
	endpoint string
}

func (s *queryResultStream) Endpoint() string {
	return s.endpoint
}

func (w *QueryComponent) Query(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateRetries(ctx, w.retries, func() (QueryResultStream, error) {
		return OrchestrateQueryEndpoint(ctx, w, opts.Endpoint,
			func(roundTripper http.RoundTripper, endpointId, endpoint, username, password string) (QueryResultStream, error) {
				res, err := cbqueryx.Query{
					Logger:    w.logger,
					UserAgent: w.userAgent,
					Transport: roundTripper,
					Endpoint:  endpoint,
					Username:  username,
					Password:  password,
				}.Query(ctx, &opts.QueryOptions)
				if err != nil {
					return nil, err
				}

				return &queryResultStream{
					ResultStream: res,
					endpoint:     endpointId,
				}, nil
			})
	})
}

func (w *QueryComponent) PreparedQuery(ctx context.Context, opts *QueryOptions) (QueryResultStream, error) {
	return OrchestrateRetries(ctx, w.retries, func() (QueryResultStream, error) {
		return OrchestrateQueryEndpoint(ctx, w, opts.Endpoint,
			func(roundTripper http.RoundTripper, endpointId, endpoint, username, password string) (QueryResultStream, error) {
				res, err := cbqueryx.PreparedQuery{
					Executor: cbqueryx.Query{
						Logger:    w.logger,
						UserAgent: w.userAgent,
						Transport: roundTripper,
						Endpoint:  endpoint,
						Username:  username,
						Password:  password,
					},
					Cache: w.preparedCache,
				}.PreparedQuery(ctx, &opts.QueryOptions)
				if err != nil {
					return nil, err
				}

				return &queryResultStream{
					ResultStream: res,
					endpoint:     endpointId,
				}, nil
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

func (w *QueryComponent) GetAllIndexes(ctx context.Context, opts *cbqueryx.GetAllIndexesOptions) ([]cbqueryx.Index, error) {
	return OrchestrateQueryMgmtCall(ctx, w, cbqueryx.Query.GetAllIndexes, opts)
}

func (w *QueryComponent) CreatePrimaryIndex(ctx context.Context, opts *cbqueryx.CreatePrimaryIndexOptions) error {
	return OrchestrateNoResQueryMgmtCall(ctx, w, cbqueryx.Query.CreatePrimaryIndex, opts)
}

func (w *QueryComponent) CreateIndex(ctx context.Context, opts *cbqueryx.CreateIndexOptions) error {
	return OrchestrateNoResQueryMgmtCall(ctx, w, cbqueryx.Query.CreateIndex, opts)
}

func (w *QueryComponent) DropPrimaryIndex(ctx context.Context, opts *cbqueryx.DropPrimaryIndexOptions) error {
	return OrchestrateNoResQueryMgmtCall(ctx, w, cbqueryx.Query.DropPrimaryIndex, opts)
}

func (w *QueryComponent) DropIndex(ctx context.Context, opts *cbqueryx.DropIndexOptions) error {
	return OrchestrateNoResQueryMgmtCall(ctx, w, cbqueryx.Query.DropIndex, opts)
}

func (w *QueryComponent) BuildDeferredIndexes(ctx context.Context, opts *cbqueryx.BuildDeferredIndexesOptions) ([]cbqueryx.DeferredIndexName, error) {
	return OrchestrateQueryMgmtCall(ctx, w, cbqueryx.Query.BuildDeferredIndexes, opts)
}
