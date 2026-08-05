package gocbcorex

import (
	"context"
	"fmt"
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
	Endpoints        map[string]string
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
	roundTripper, _, endpoint, username, password, err := w.SelectEndpoint(nil)
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
				Auth: &cbhttpx.BasicAuth{
					Username: username,
					Password: password,
				},
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
				Auth: &cbhttpx.BasicAuth{
					Username: username,
					Password: password,
				},
			}, ctx, opts)
		})
	return err
}

type GetMgmtEndpointResult struct {
	RoundTripper http.RoundTripper
	Endpoint     string
	Username     string
	Password     string
}

func (w *MgmtComponent) GetEndpoint(ctx context.Context) (*GetMgmtEndpointResult, error) {
	return OrchestrateMgmtEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (*GetMgmtEndpointResult, error) {
			return &GetMgmtEndpointResult{
				RoundTripper: roundTripper,
				Endpoint:     endpoint,
				Username:     username,
				Password:     password,
			}, nil
		})
}

func (w *MgmtComponent) GetClusterInfo(ctx context.Context, opts *cbmgmtx.GetClusterInfoOptions) (*cbmgmtx.ClusterInfoResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetClusterInfo, opts)
}

type GetAggregatedClusterInfoOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) GetAggregatedClusterInfo(ctx context.Context, opts *GetAggregatedClusterInfoOptions) (*cbmgmtx.AggregatedClusterInfoResponse, error) {
	hlpr := cbmgmtx.ClusterInfoHelper{
		Logger:     w.logger.Named("cluster-info"),
		UserAgent:  w.userAgent,
		OnBehalfOf: opts.OnBehalfOf,
	}

	roundTripper, targets, err := w.GetAllTargets(nil)
	if err != nil {
		return nil, err
	}

	return hlpr.FetchAll(ctx, &cbmgmtx.GetAggregatedClusterInfoOptions{
		Transport: roundTripper,
		Targets:   baseHttpTargets(targets).ToMgmtx(),
	})
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

func (w *MgmtComponent) CheckBucketExists(ctx context.Context, opts *cbmgmtx.CheckBucketExistsOptions) (bool, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.CheckBucketExists, opts)
}

func (w *MgmtComponent) XdcrC2c(ctx context.Context, opts *cbmgmtx.XdcrC2cOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.XdcrC2c, opts)
}

type EnsureBucketOptions struct {
	BucketName   string
	BucketUUID   string
	WantMissing  bool
	WantHealthy  bool
	WantSettings *cbmgmtx.MutableBucketSettings
	OnBehalfOf   *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureBucket(ctx context.Context, opts *EnsureBucketOptions) error {
	hlpr := cbmgmtx.EnsureBucketHelper{
		Logger:       w.logger.Named("ensure-bucket"),
		UserAgent:    w.userAgent,
		OnBehalfOf:   opts.OnBehalfOf,
		BucketName:   opts.BucketName,
		BucketUUID:   opts.BucketUUID,
		WantHealthy:  opts.WantHealthy,
		WantMissing:  opts.WantMissing,
		WantSettings: opts.WantSettings,
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

type EnsureManifestNsOnlyOptions struct {
	BucketName  string
	ManifestUid uint64
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureManifestNsOnly(ctx context.Context, opts *EnsureManifestNsOnlyOptions) error {
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

type EnsureManifestOptions struct {
	BucketName  string
	ManifestUid uint64
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureManifest(ctx context.Context, opts *EnsureManifestOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, func(h cbmgmtx.Management, ctx context.Context, req *cbmgmtx.EnsureManifestOptions) error {
		return h.EnsureManifest(ctx, req)
	}, &cbmgmtx.EnsureManifestOptions{
		BucketName:  opts.BucketName,
		ManifestUid: fmt.Sprintf("%x", opts.ManifestUid),
		OnBehalfOf:  opts.OnBehalfOf,
	})
}

func (w *MgmtComponent) GetAllUsers(ctx context.Context, opts *cbmgmtx.GetAllUsersOptions) ([]*cbmgmtx.UserJson, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetAllUsers, opts)
}

func (w *MgmtComponent) GetUser(ctx context.Context, opts *cbmgmtx.GetUserOptions) (*cbmgmtx.UserJson, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetUser, opts)
}

func (w *MgmtComponent) UpsertUser(ctx context.Context, opts *cbmgmtx.UpsertUserOptions) (*cbmgmtx.UpsertUserResult, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.UpsertUser, opts)
}

func (w *MgmtComponent) DeleteUser(ctx context.Context, opts *cbmgmtx.DeleteUserOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.DeleteUser, opts)
}

func (w *MgmtComponent) UpsertUserGroup(ctx context.Context, opts *cbmgmtx.UpsertUserGroupOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.UpsertUserGroup, opts)
}

func (w *MgmtComponent) GetUserGroup(ctx context.Context, opts *cbmgmtx.GetUserGroupOptions) (*cbmgmtx.UserGroupJson, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetUserGroup, opts)
}

func (w *MgmtComponent) DeleteUserGroup(ctx context.Context, opts *cbmgmtx.DeleteUserGroupOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.DeleteUserGroup, opts)
}

type EnsureUserOptions struct {
	Username    string
	Domain      cbmgmtx.AuthDomain
	WantMissing bool
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo

	// SincePasswordChanged, if set, causes EnsureUser to additionally wait
	// until the password change has propagated to all nodes, in addition to
	// the usual existence check. Populate this from
	// UpsertUserResult.PreviousPasswordChanged. Leave unset for any use of
	// EnsureUser that isn't specifically confirming a password change (this
	// is meaningless in combination with WantMissing).
	SincePasswordChanged *cbmgmtx.PasswordChangedMarker

	// WantSettings, if set, causes EnsureUser to additionally wait until the
	// given roles and/or groups have propagated to all nodes. Populate this
	// from the same Roles/Groups passed to UpsertUser.
	WantSettings *cbmgmtx.WantUserSettings
}

func (w *MgmtComponent) EnsureUser(ctx context.Context, opts *EnsureUserOptions) error {
	hlpr := cbmgmtx.EnsureUserHelper{
		Logger:               w.logger.Named("ensure-user"),
		UserAgent:            w.userAgent,
		OnBehalfOf:           opts.OnBehalfOf,
		Username:             opts.Username,
		Domain:               opts.Domain,
		WantMissing:          opts.WantMissing,
		SincePasswordChanged: opts.SincePasswordChanged,
		WantSettings:         opts.WantSettings,
	}

	b := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, b, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.Poll(ctx, &cbmgmtx.EnsureUserPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToMgmtx(),
		})
	})
}

type EnsureUserGroupOptions struct {
	GroupName   string
	WantMissing bool
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) EnsureUserGroup(ctx context.Context, opts *EnsureUserGroupOptions) error {
	hlpr := cbmgmtx.EnsureUserGroupHelper{
		Logger:      w.logger.Named("ensure-user-group"),
		UserAgent:   w.userAgent,
		OnBehalfOf:  opts.OnBehalfOf,
		GroupName:   opts.GroupName,
		WantMissing: opts.WantMissing,
	}

	b := ExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	return w.ensureResource(ctx, b, func(ctx context.Context, roundTripper http.RoundTripper,
		ensureTargets baseHttpTargets) (bool, error) {
		return hlpr.Poll(ctx, &cbmgmtx.EnsureUserGroupPollOptions{
			Transport: roundTripper,
			Targets:   ensureTargets.ToMgmtx(),
		})
	})
}

func (w *MgmtComponent) GetMetaKv2(ctx context.Context, opts *cbmgmtx.GetMetaKv2Options) (*cbmgmtx.GetMetaKv2Response, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetMetaKv2, opts)
}

func (w *MgmtComponent) PutMetaKv2(ctx context.Context, opts *cbmgmtx.PutMetaKv2Options) (*cbmgmtx.PutMetaKv2Response, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.PutMetaKv2, opts)
}

func (w *MgmtComponent) DeleteMetaKv2(ctx context.Context, opts *cbmgmtx.DeleteMetaKv2Options) (*cbmgmtx.DeleteMetaKv2Response, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.DeleteMetaKv2, opts)
}

func (w *MgmtComponent) GetMetaKv2Snapshot(ctx context.Context, opts *cbmgmtx.GetMetaKv2SnapshotOptions) (*cbmgmtx.GetMetaKv2SnapshotResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetMetaKv2Snapshot, opts)
}

func (w *MgmtComponent) SetMetaKv2Multiple(ctx context.Context, opts *cbmgmtx.SetMetaKv2MultipleOptions) (*cbmgmtx.SetMetaKv2MultipleResponse, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.SetMetaKv2Multiple, opts)
}

func (w *MgmtComponent) SyncMetaKv2Quorum(ctx context.Context, opts *cbmgmtx.SyncMetaKv2QuorumOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.SyncMetaKv2Quorum, opts)
}

type WatchMetaKv2Options struct {
	Path         string
	PollInterval time.Duration
	OnBehalfOf   *cbhttpx.OnBehalfOfInfo
}

func (w *MgmtComponent) WatchMetaKv2(ctx context.Context, opts *WatchMetaKv2Options) (<-chan struct{}, error) {
	if opts == nil {
		opts = &WatchMetaKv2Options{}
	}

	epRes, err := w.GetEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	mgmt := cbmgmtx.Management{
		Transport: epRes.RoundTripper,
		UserAgent: w.userAgent,
		Endpoint:  epRes.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: epRes.Username,
			Password: epRes.Password,
		},
	}

	hlpr := cbmgmtx.MetaKvWatchHelper{
		Logger:       w.logger.Named("metakv-watch"),
		UserAgent:    w.userAgent,
		OnBehalfOf:   opts.OnBehalfOf,
		PollInterval: opts.PollInterval,
		Path:         opts.Path,
	}

	return hlpr.Watch(ctx, &cbmgmtx.MetaKvWatchOptions{
		Management: mgmt,
	})
}
