package gocbcorex

import (
	"context"
	"errors"
	"net/http"
	"time"

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
				Username:  username,
				Password:  password,
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
				Username:  username,
				Password:  password,
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

		// On initial startup it's possible for the bucket (if applicable) to be in a state
		// where it isn't "ready" yet and is sending us an empty vbucket map. When this happens
		// we will retry fetching a config until we get one with a valid vbucket map.
		if parsedConfig.BucketName == "" || parsedConfig.VbucketMap.IsValid() {
			break
		}

		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(1 * time.Millisecond):
		}
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
