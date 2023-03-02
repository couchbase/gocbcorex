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

func (w *MgmtComponent) GetCollectionManifest(ctx context.Context, bucketName string) (*cbmgmtx.CollectionManifestJson, error) {
	return OrchestrateMgmtEndpoint(ctx, w,
		func(roundTripper http.RoundTripper, endpoint, username, password string) (*cbmgmtx.CollectionManifestJson, error) {
			return cbmgmtx.Management{
				UserAgent: w.userAgent,
				Transport: roundTripper,
				Endpoint:  endpoint,
				Username:  username,
				Password:  password,
			}.GetCollectionManifest(ctx, bucketName)
		})
}
