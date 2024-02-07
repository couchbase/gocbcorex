package gocbcorex

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

type MgmtComponent struct {
	baseHttpComponent

	logger *zap.Logger
}

type MgmtComponentConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type MgmtComponentOptions struct {
	Logger    *zap.Logger
	UserAgent string
}

func OrchestrateMgmtEndpoint[RespT any](
	ctx context.Context,
	w *MgmtComponent,
	fn func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error),
) (RespT, error) {
	roundTripper, endpoint, username, password, err := w.SelectEndpoint(nil)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	if endpoint == "" {
		var emptyResp RespT
		return emptyResp, serviceNotAvailableError{Service: ServiceTypeMgmt}
	}

	return fn(roundTripper, endpoint, username, password)
}

func NewMgmtComponent(retries RetryManager, config *MgmtComponentConfig, opts *MgmtComponentOptions) *MgmtComponent {
	return &MgmtComponent{
		baseHttpComponent: baseHttpComponent{
			serviceType: ServiceTypeMgmt,
			userAgent:   opts.UserAgent,
			state: &baseHttpComponentState{
				httpRoundTripper: config.HttpRoundTripper,
				endpoints:        config.Endpoints,
				authenticator:    config.Authenticator,
			},
		},
		logger: opts.Logger,
	}
}

func (w *MgmtComponent) Reconfigure(config *MgmtComponentConfig) error {
	w.updateState(baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	})
	return nil
}

func OrchestrateSimpleMgmtCall[OptsT any, RespT any](
	ctx context.Context,
	w *MgmtComponent,
	execFn func(o cbmgmtx.Management, ctx context.Context, req OptsT) (RespT, error),
	opts OptsT,
) (RespT, error) {
	return OrchestrateMgmtEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (RespT, error) {
			return execFn(cbmgmtx.Management{
				UserAgent: w.userAgent,
				Transport: roundTripper,
				Endpoint:  endpoint,
				Username:  username,
				Password:  password,
			}, ctx, opts)
		})
}

func OrchestrateNoResMgmtCall[OptsT any](
	ctx context.Context,
	w *MgmtComponent,
	execFn func(o cbmgmtx.Management, ctx context.Context, req OptsT) error,
	opts OptsT,
) error {
	_, err := OrchestrateMgmtEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (interface{}, error) {
			return nil, execFn(cbmgmtx.Management{
				UserAgent: w.userAgent,
				Transport: roundTripper,
				Endpoint:  endpoint,
				Username:  username,
				Password:  password,
			}, ctx, opts)
		})
	return err
}

func (w *MgmtComponent) GetCollectionManifest(ctx context.Context, opts *cbmgmtx.GetCollectionManifestOptions) (*cbconfig.CollectionManifestJson, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetCollectionManifest, opts)
}

func (w *MgmtComponent) CreateScope(ctx context.Context, opts *cbmgmtx.CreateScopeOptions) (*cbmgmtx.CreateScopeResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.CreateScope, opts)
}

func (w *MgmtComponent) DeleteScope(ctx context.Context, opts *cbmgmtx.DeleteScopeOptions) (*cbmgmtx.DeleteScopeResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.DeleteScope, opts)
}

func (w *MgmtComponent) CreateCollection(ctx context.Context, opts *cbmgmtx.CreateCollectionOptions) (*cbmgmtx.CreateCollectionResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.CreateCollection, opts)
}

func (w *MgmtComponent) DeleteCollection(ctx context.Context, opts *cbmgmtx.DeleteCollectionOptions) (*cbmgmtx.DeleteCollectionResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.DeleteCollection, opts)
}

func (w *MgmtComponent) UpdateCollection(ctx context.Context, opts *cbmgmtx.UpdateCollectionOptions) (*cbmgmtx.UpdateCollectionResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.UpdateCollection, opts)
}

func (w *MgmtComponent) GetAllBuckets(ctx context.Context, opts *cbmgmtx.GetAllBucketsOptions) ([]*cbmgmtx.BucketDef, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetAllBuckets, opts)
}

func (w *MgmtComponent) GetBucket(ctx context.Context, opts *cbmgmtx.GetBucketOptions) (*cbmgmtx.BucketDef, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetBucket, opts)
}

func (w *MgmtComponent) CreateBucket(ctx context.Context, opts *cbmgmtx.CreateBucketOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.CreateBucket, opts)
}

func (w *MgmtComponent) UpdateBucket(ctx context.Context, opts *cbmgmtx.UpdateBucketOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.UpdateBucket, opts)
}

func (w *MgmtComponent) FlushBucket(ctx context.Context, opts *cbmgmtx.FlushBucketOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.FlushBucket, opts)
}

func (w *MgmtComponent) DeleteBucket(ctx context.Context, opts *cbmgmtx.DeleteBucketOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.DeleteBucket, opts)
}

type EnsureBucketOptions struct {
	BucketName  string
	BucketUUID  string
	WantMissing bool
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureBucket(ctx context.Context, opts *EnsureBucketOptions) error {
	hlpr := cbmgmtx.EnsureBucketHelper{
		Logger:      w.logger.Named("ensure-bucket"),
		UserAgent:   w.userAgent,
		OnBehalfOf:  opts.OnBehalfOf,
		BucketName:  opts.BucketName,
		BucketUUID:  opts.BucketUUID,
		WantMissing: opts.WantMissing,
	}

	b := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, b, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.Poll(ctx, &cbmgmtx.EnsureBucketPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToMgmtx(),
		})
	})
}

type EnsureManifestOptions struct {
	BucketName  string
	ManifestUid uint64
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureManifest(ctx context.Context, opts *EnsureManifestOptions) error {
	hlpr := cbmgmtx.EnsureManifestHelper{
		Logger:      w.logger.Named("ensure-manifest"),
		UserAgent:   w.userAgent,
		OnBehalfOf:  opts.OnBehalfOf,
		BucketName:  opts.BucketName,
		ManifestUid: opts.ManifestUid,
	}

	b := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, b, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.Poll(ctx, &cbmgmtx.EnsureManifestPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToMgmtx(),
		})
	})
}
