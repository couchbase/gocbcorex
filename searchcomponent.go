package gocbcorex

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
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
		return emptyResp, serviceNotAvailableError{Service: ServiceTypeSearch}
	}

	return fn(roundTripper, endpoint, username, password)
}

func OrchestrateNoResSearchMgmtCall[OptsT any](
	ctx context.Context,
	w *SearchComponent,
	execFn func(o cbsearchx.Search, ctx context.Context, req OptsT) error,
	opts OptsT,
) error {
	return OrchestrateNoResponseRetries(ctx, w.retries, func() error {
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
	})
}

func OrchestrateSearchMgmtCall[OptsT any, RespT any](
	ctx context.Context,
	w *SearchComponent,
	execFn func(o cbsearchx.Search, ctx context.Context, req OptsT) (RespT, error),
	opts OptsT,
) (RespT, error) {
	return OrchestrateSearchRetries(ctx, w.retries, func() (RespT, error) {
		return OrchestrateSearchEndpoint(ctx, w,
			func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error) {
				return execFn(cbsearchx.Search{
					UserAgent: w.userAgent,
					Transport: roundTripper,
					Endpoint:  endpoint,
					Username:  username,
					Password:  password,
				}, ctx, opts)
			})
	})
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
	w.updateState(baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	})
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
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.UpsertIndex, opts)
}

func (w *SearchComponent) DeleteIndex(ctx context.Context, opts *cbsearchx.DeleteIndexOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.DeleteIndex, opts)
}

func (w *SearchComponent) GetIndex(ctx context.Context, opts *cbsearchx.GetIndexOptions) (*cbsearchx.Index, error) {
	return OrchestrateSearchMgmtCall(ctx, w, cbsearchx.Search.GetIndex, opts)
}

func (w *SearchComponent) GetAllIndexes(ctx context.Context, opts *cbsearchx.GetAllIndexesOptions) ([]cbsearchx.Index, error) {
	return OrchestrateSearchMgmtCall(ctx, w, cbsearchx.Search.GetAllIndexes, opts)
}

func (w *SearchComponent) AnalyzeDocument(ctx context.Context, opts *cbsearchx.AnalyzeDocumentOptions) (*cbsearchx.DocumentAnalysis, error) {
	return OrchestrateSearchMgmtCall(ctx, w, cbsearchx.Search.AnalyzeDocument, opts)
}

func (w *SearchComponent) GetIndexedDocumentsCount(ctx context.Context, opts *cbsearchx.GetIndexedDocumentsCountOptions) (uint64, error) {
	return OrchestrateSearchMgmtCall(ctx, w, cbsearchx.Search.GetIndexedDocumentsCount, opts)
}

func (w *SearchComponent) PauseIngest(ctx context.Context, opts *cbsearchx.PauseIngestOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.PauseIngest, opts)
}

func (w *SearchComponent) ResumeIngest(ctx context.Context, opts *cbsearchx.ResumeIngestOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.ResumeIngest, opts)
}

func (w *SearchComponent) AllowQuerying(ctx context.Context, opts *cbsearchx.AllowQueryingOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.AllowQuerying, opts)
}

func (w *SearchComponent) DisallowQuerying(ctx context.Context, opts *cbsearchx.DisallowQueryingOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.DisallowQuerying, opts)
}

func (w *SearchComponent) FreezePlan(ctx context.Context, opts *cbsearchx.FreezePlanOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.FreezePlan, opts)
}

func (w *SearchComponent) UnfreezePlan(ctx context.Context, opts *cbsearchx.UnfreezePlanOptions) error {
	return OrchestrateNoResSearchMgmtCall(ctx, w, cbsearchx.Search.UnfreezePlan, opts)
}

type EnsureSearchIndexCreatedOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (w *SearchComponent) EnsureIndexCreated(ctx context.Context, opts *EnsureSearchIndexCreatedOptions) error {
	hlpr := cbsearchx.EnsureIndexHelper{
		Logger:     w.logger.Named("ensure-index"),
		UserAgent:  w.userAgent,
		OnBehalfOf: opts.OnBehalfOf,
		BucketName: opts.BucketName,
		ScopeName:  opts.ScopeName,
		IndexName:  opts.IndexName,
	}

	backoff := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, backoff, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.PollCreated(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToSearchx(),
		})
	})
}

type EnsureSearchIndexDroppedOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (w *SearchComponent) EnsureIndexDropped(ctx context.Context, opts *EnsureSearchIndexDroppedOptions) error {
	hlpr := cbsearchx.EnsureIndexHelper{
		Logger:     w.logger.Named("ensure-index"),
		UserAgent:  w.userAgent,
		OnBehalfOf: opts.OnBehalfOf,
		BucketName: opts.BucketName,
		ScopeName:  opts.ScopeName,
		IndexName:  opts.IndexName,
	}

	backoff := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, backoff, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.PollDropped(ctx, &cbsearchx.EnsureIndexPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToSearchx(),
		})
	})
}
