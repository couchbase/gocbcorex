package gocbcorex

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/zaputils"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"go.uber.org/zap"
)

type ConfigWatcherHttpConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
	BucketName       string
}

type ConfigWatcherHttpOptions struct {
	Logger *zap.Logger
}

type configWatcherHttpState struct {
	httpRoundTripper http.RoundTripper
	endpoints        []string
	userAgent        string
	authenticator    Authenticator
	bucketName       string
}

type ConfigWatcherHttp struct {
	logger *zap.Logger

	lock  sync.Mutex
	state *configWatcherHttpState
}

func NewConfigWatcherHttp(config *ConfigWatcherHttpConfig, opts *ConfigWatcherHttpOptions) (*ConfigWatcherHttp, error) {
	return &ConfigWatcherHttp{
		logger: opts.Logger,
		state: &configWatcherHttpState{
			httpRoundTripper: config.HttpRoundTripper,
			endpoints:        config.Endpoints,
			userAgent:        config.UserAgent,
			authenticator:    config.Authenticator,
			bucketName:       config.BucketName,
		},
	}, nil
}

func (w *ConfigWatcherHttp) Reconfigure(config *ConfigWatcherHttpConfig) error {
	w.lock.Lock()
	w.state = &configWatcherHttpState{
		httpRoundTripper: config.HttpRoundTripper,
		endpoints:        config.Endpoints,
		userAgent:        config.UserAgent,
		authenticator:    config.Authenticator,
		bucketName:       config.BucketName,
	}
	w.lock.Unlock()
	return nil
}

func configWatcherHttp_pollOne(
	ctx context.Context,
	logger *zap.Logger,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
	bucketName string,
) (*ParsedConfig, error) {
	hostPort, err := getHostFromUri(endpoint)
	if err != nil {
		return nil, err
	}

	hostname, err := hostFromHostPort(hostPort)
	if err != nil {
		return nil, err
	}

	logger.Debug("Polling for new config",
		zap.String("hostPort", hostPort),
		zap.String("endpoint", endpoint))

	username, password, err := authenticator.GetCredentials(ServiceTypeMgmt, hostPort)
	if err != nil {
		return nil, err
	}

	var parsedConfig *ParsedConfig
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
			return nil, err
		}

		logger.Debug("Poller fetched new config",
			zap.Int("config", resp.Rev),
			zap.Int("configRevEpoch", resp.RevEpoch))

		parsedConfig, err = ConfigParser{}.ParseTerseConfig(resp, hostname)
		if err != nil {
			return nil, err
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
			return nil, err
		}

		logger.Debug("Poller fetched new config",
			zap.Int("config", resp.Rev),
			zap.Int("configRevEpoch", resp.RevEpoch))

		parsedConfig, err = ConfigParser{}.ParseTerseConfig(resp, hostname)
		if err != nil {
			return nil, err
		}
	}

	return parsedConfig, nil
}

func (w *ConfigWatcherHttp) watchThread(ctx context.Context, outCh chan<- *ParsedConfig) {
	var lastSentConfig *ParsedConfig
	var recentEndpoints []string
	allEndpointsFailed := true

	for ctx.Err() == nil {
		w.lock.Lock()
		state := w.state
		w.lock.Unlock()

		// if there are no endpoints to poll, we need to sleep and wait
		if len(state.endpoints) == 0 {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}

			continue
		}

		// remove the most recently polled endpoints
		remainingEndpoints := filterStringsOut(state.endpoints, recentEndpoints)

		// if there are no endpoints left, we reset the lists
		if len(remainingEndpoints) == 0 {
			if allEndpointsFailed {
				// if all the endpoints failed in a row, we do a sleep to ensure
				// we don't loop for no reason
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
				}
			}

			recentEndpoints = nil
			allEndpointsFailed = true

			continue
		}

		endpoint := remainingEndpoints[0]
		recentEndpoints = append(recentEndpoints, endpoint)

		pollCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
		parsedConfig, err := configWatcherHttp_pollOne(
			pollCtx,
			w.logger,
			state.httpRoundTripper,
			endpoint,
			state.userAgent,
			state.authenticator,
			state.bucketName)
		cancel()
		if err != nil {
			w.logger.Debug("failed to poll config via http",
				zap.Error(err),
				zap.String("endpoint", endpoint),
				zaputils.BucketName("bucketName", state.bucketName))
			continue
		}

		allEndpointsFailed = false

		// we do some deduplication here to avoid spamming consumers with logs
		// with this implementation which polls rather than streams.
		if lastSentConfig != nil && parsedConfig.Compare(lastSentConfig) <= 0 {
			// we already dispatched an identical config
		} else {
			outCh <- parsedConfig
			lastSentConfig = parsedConfig
		}

		// after successfully receiving a configuration, we wait 5 seconds
		// before polling the next server.
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
		}
	}

	close(outCh)
}

func (w *ConfigWatcherHttp) Watch(ctx context.Context) <-chan *ParsedConfig {
	outCh := make(chan *ParsedConfig, 1)
	go w.watchThread(ctx, outCh)
	return outCh
}
