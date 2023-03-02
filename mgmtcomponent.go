package gocbcorex

import (
	"context"
	"net/http"

	"github.com/couchbase/gocbcorex/cbmgmtx"
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
	var recentEndpoints []string

	for {
		roundTripper, endpoint, username, password, err := w.SelectEndpoint(recentEndpoints)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		if endpoint == "" {
			var emptyResp RespT
			return emptyResp, ErrServiceNotAvailable
		}

		// mark the selected endpoint as having been tried
		recentEndpoints = append(recentEndpoints, endpoint)

		res, err := fn(roundTripper, endpoint, username, password)
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

func NewMgmtComponent(retries RetryManager, config *MgmtComponentConfig, opts *MgmtComponentOptions) *MgmtComponent {
	return &MgmtComponent{
		baseHttpComponent: baseHttpComponent{
			serviceType: MgmtService,
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
	w.lock.Lock()
	w.state = &baseHttpComponentState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		authenticator:    config.Authenticator,
	}
	w.lock.Unlock()
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

func (w *MgmtComponent) GetCollectionManifest(ctx context.Context, opts *cbmgmtx.GetCollectionManifestOptions) (*cbmgmtx.CollectionManifestJson, error) {
	return OrchestrateSimpleMgmtCall(ctx, w, cbmgmtx.Management.GetCollectionManifest, opts)
}

func (w *MgmtComponent) CreateScope(ctx context.Context, opts *cbmgmtx.CreateScopeOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.CreateScope, opts)
}

func (w *MgmtComponent) DeleteScope(ctx context.Context, opts *cbmgmtx.DeleteScopeOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.DeleteScope, opts)
}

func (w *MgmtComponent) CreateCollection(ctx context.Context, opts *cbmgmtx.CreateCollectionOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.CreateCollection, opts)
}

func (w *MgmtComponent) DeleteCollection(ctx context.Context, opts *cbmgmtx.DeleteCollectionOptions) error {
	return OrchestrateNoResMgmtCall(ctx, w, cbmgmtx.Management.DeleteCollection, opts)
}
