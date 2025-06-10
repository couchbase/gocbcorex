package gocbcorex

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"go.uber.org/zap"
)

type ConfigBoostrapHttpOptions struct {
	Logger           *zap.Logger
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
	BucketName       string
}

type ConfigBootstrapHttp struct {
	logger           *zap.Logger
	httpRoundTripper http.RoundTripper
	endpoints        []string
	userAgent        string
	authenticator    Authenticator
	bucketName       string
}

func NewConfigBootstrapHttp(opts ConfigBoostrapHttpOptions) (*ConfigBootstrapHttp, error) {
	return &ConfigBootstrapHttp{
		logger:           opts.Logger,
		httpRoundTripper: opts.HttpRoundTripper,
		endpoints:        opts.Endpoints,
		userAgent:        opts.UserAgent,
		authenticator:    opts.Authenticator,
		bucketName:       opts.BucketName,
	}, nil
}

func configBootstrapHttp_bootstrapOne(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
	bucketName string,
) (*ParsedConfig, string, error) {
	hostport, err := getHostFromUri(endpoint)
	if err != nil {
		return nil, "", err
	}

	var parsedConfig *ParsedConfig
	for {
		username, password, err := authenticator.GetCredentials(ServiceTypeMgmt, hostport)
		if err != nil {
			return nil, "", err
		}

		hostOnly, err := hostFromHostPort(hostport)
		if err != nil {
			return nil, "", err
		}

		if bucketName == "" {
			resp, err := cbmgmtx.Management{
				Transport: httpRoundTripper,
				UserAgent: userAgent,
				Endpoint:  endpoint,
				Auth: &cbhttpx.BasicAuth{
					Username: username,
					Password: password,
				},
			}.GetTerseClusterConfig(ctx, &cbmgmtx.GetTerseClusterConfigOptions{})
			if err != nil {
				return nil, "", err
			}

			parsedConfig, err = ConfigParser{}.ParseTerseConfig(resp, hostOnly)
			if err != nil {
				return nil, "", err
			}
		} else {
			resp, err := cbmgmtx.Management{
				Transport: httpRoundTripper,
				UserAgent: userAgent,
				Endpoint:  endpoint,
				Auth: &cbhttpx.BasicAuth{
					Username: username,
					Password: password,
				},
			}.GetTerseBucketConfig(ctx, &cbmgmtx.GetTerseBucketConfigOptions{
				BucketName: bucketName,
			})
			if err != nil {
				return nil, "", err
			}

			parsedConfig, err = ConfigParser{}.ParseTerseConfig(resp, hostOnly)
			if err != nil {
				return nil, "", err
			}
		}

		if parsedConfig.BucketType == bktTypeCouchbase && parsedConfig.VbucketMap == nil {
			// This is a transient scenario that can occur when a bucket is initially warming
			// up.  Instead of failing bootstrap for this, we instead sleep for a bit and then
			// try bootstrapping again.
			select {
			case <-ctx.Done():
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, "", &contextualDeadline{"bucket warming up and has no vbucket map"}
				} else {
					return nil, "", err
				}
			case <-time.After(1 * time.Millisecond):
			}

			continue
		}

		break
	}

	networkType := NetworkTypeHeuristic{}.Identify(parsedConfig, hostport)

	return parsedConfig, networkType, nil
}

func (w ConfigBootstrapHttp) Bootstrap(ctx context.Context) (*ParsedConfig, string, error) {
	attemptErrs := make(map[string]error)
	for _, endpoint := range w.endpoints {
		var err error
		parsedConfig, networkType, err := configBootstrapHttp_bootstrapOne(
			ctx,
			w.httpRoundTripper,
			endpoint,
			w.userAgent,
			w.authenticator,
			w.bucketName,
		)
		if err != nil {
			if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
				return nil, "", err
			}

			attemptErrs[endpoint] = err
			continue
		}

		return parsedConfig, networkType, nil
	}

	return nil, "", &BootstrapAllFailedError{
		Errors: attemptErrs,
	}
}
