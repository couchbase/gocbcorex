package gocbcorex

import (
	"context"
	"net/http"

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

	username, password, err := authenticator.GetCredentials(MgmtService, hostport)
	if err != nil {
		return nil, "", err
	}

	hostOnly, err := hostFromHostPort(hostport)
	if err != nil {
		return nil, "", err
	}

	var parsedConfig *ParsedConfig
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

	networkType := NetworkTypeHeuristic{}.Identify(parsedConfig, hostport)

	return parsedConfig, networkType, nil
}

func (w ConfigBootstrapHttp) Bootstrap(ctx context.Context) (*ParsedConfig, string, error) {
	if len(w.endpoints) == 1 {
		return configBootstrapHttp_bootstrapOne(
			ctx,
			w.httpRoundTripper,
			w.endpoints[0],
			w.userAgent,
			w.authenticator,
			w.bucketName,
		)
	}

	attemptErrs := make(map[string]error)

	for _, endpoint := range w.endpoints {
		parsedConfig, networkType, err := configBootstrapHttp_bootstrapOne(
			ctx,
			w.httpRoundTripper,
			endpoint,
			w.userAgent,
			w.authenticator,
			w.bucketName,
		)
		if err != nil {
			attemptErrs[endpoint] = err
			continue
		}

		return parsedConfig, networkType, nil
	}

	return nil, "", &BootstrapAllFailedError{
		Errors: attemptErrs,
	}
}
