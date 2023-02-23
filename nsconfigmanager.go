package gocbcorex

import (
	"context"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

type NsConfigManagerOptions struct {
	Logger        *zap.Logger
	HttpClient    *http.Client
	Endpoint      string
	UserAgent     string
	Authenticator Authenticator
}

type NsConfigManager struct {
	logger *zap.Logger

	httpClient    *http.Client
	endpointHost  string
	endpoint      string
	userAgent     string
	authenticator Authenticator
}

func NewNsConfigManager(opts NsConfigManagerOptions) (*NsConfigManager, error) {
	host, err := getHostFromUri(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	return &NsConfigManager{
		logger:        opts.Logger,
		httpClient:    opts.HttpClient,
		endpoint:      opts.Endpoint,
		endpointHost:  host,
		userAgent:     opts.UserAgent,
		authenticator: opts.Authenticator,
	}, nil
}

func (w NsConfigManager) Bootstrap(ctx context.Context) (*ParsedConfig, error) {
	username, password, err := w.authenticator.GetCredentials(MgmtService, w.endpointHost)
	if err != nil {
		return nil, err
	}

	resp, err := cbhttpx.HttpManagement{
		HttpClient: w.httpClient,
		UserAgent:  w.userAgent,
		Endpoint:   w.endpoint,
		Username:   username,
		Password:   password,
	}.GetTerseClusterConfig(ctx)
	if err != nil {
		return nil, err
	}

	parsedConfig, err := ConfigParser{}.ParseTerseConfig(resp, w.endpointHost)
	if err != nil {
		return nil, err
	}

	return parsedConfig, nil
}

func (w NsConfigManager) watchClusterConfigOnce(
	ctx context.Context,
	firstConfigTimeout time.Duration,
) (*cbconfig.TerseConfigJson, cbhttpx.TerseClusterConfig_Stream, error) {
	cancelCtx, cancel := context.WithCancel(ctx)

	firstRespTimer := time.AfterFunc(firstConfigTimeout, func() {
		cancel()
	})

	resp, err := cbhttpx.HttpManagement{
		HttpClient: &http.Client{},
		UserAgent:  "gocbcorex/...",
		Endpoint:   "http://192.168.0.100:8091",
		Username:   "Administrator",
		Password:   "password",
	}.StreamTerseClusterConfig(cancelCtx)
	if err != nil {
		firstRespTimer.Stop()
		cancel()
		return nil, nil, err
	}

	firstConfig, err := resp.Recv()
	if err != nil {
		firstRespTimer.Stop()
		cancel()
		return nil, nil, err
	}

	firstRespTimer.Stop()

	return firstConfig, resp, nil
}

func (w NsConfigManager) Watch(ctx context.Context) <-chan *ParsedConfig {
	outCh := make(chan *ParsedConfig)
	go func() {
		for {
			// if the parent context is closed, we stop looping
			if ctx.Err() != nil {
				close(outCh)
				return
			}

			// each stream is only permitted to live for 60s before it's restarted
			cancelCtx, cancel := context.WithTimeout(ctx, 60*time.Second)

			// start watching the stream
			firstConfig, stream, err := w.watchClusterConfigOnce(cancelCtx, 5*time.Second)
			if err != nil {
				// we failed to start listening to this server
				cancel()
				continue
			}

			// try to parse the first config we received
			firstParsedConfig, err := ConfigParser{}.ParseTerseConfig(firstConfig, w.endpointHost)
			if err != nil {
				// the config emitted by this server was faulty
				cancel()
				continue
			}

			// send the first received config to the update channel
			outCh <- firstParsedConfig

			for {
				config, err := stream.Recv()
				if err != nil {
					// there was an error receiving more configurations
					break
				}

				// try to parse the first config we received
				parsedConfig, err := ConfigParser{}.ParseTerseConfig(config, w.endpointHost)
				if err != nil {
					// the config emitted by this server was faulty
					break
				}

				outCh <- parsedConfig
			}

			cancel()
		}
	}()
	return outCh
}
