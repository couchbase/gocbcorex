package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type ConfigWatcherMemdConfig struct {
	Endpoints []string
}

type ConfigWatcherMemdOptions struct {
	Logger          *zap.Logger
	KvClientManager KvClientManager
	PollingPeriod   time.Duration
}

type configWatcherMemdState struct {
	endpoints []string
}

type ConfigWatcherMemd struct {
	logger          *zap.Logger
	kvClientManager KvClientManager
	pollingPeriod   time.Duration

	lock  sync.Mutex
	state *configWatcherMemdState
}

func NewConfigWatcherMemd(config *ConfigWatcherMemdConfig, opts *ConfigWatcherMemdOptions) (*ConfigWatcherMemd, error) {
	return &ConfigWatcherMemd{
		logger:          opts.Logger,
		kvClientManager: opts.KvClientManager,
		pollingPeriod:   opts.PollingPeriod,
		state: &configWatcherMemdState{
			endpoints: config.Endpoints,
		},
	}, nil
}

func (w *ConfigWatcherMemd) Reconfigure(config *ConfigWatcherMemdConfig) error {
	w.lock.Lock()
	w.state = &configWatcherMemdState{
		endpoints: config.Endpoints,
	}
	w.lock.Unlock()
	return nil
}

func configWatcherMemd_pollOne(
	ctx context.Context,
	logger *zap.Logger,
	kvClientManager KvClientManager,
	endpoint string,
) (*ParsedConfig, error) {
	client, err := kvClientManager.GetClient(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	host, _, _ := client.RemoteHostPort()
	if host == "" {
		return nil, errors.New("unexpected cccp endpoint format")
	}

	logger.Debug("Polling for new config",
		zap.String("host", host),
		zap.String("endpoint", endpoint))

	resp, err := client.GetClusterConfig(ctx, &memdx.GetClusterConfigRequest{})
	if err != nil {
		return nil, err
	}

	var config cbconfig.TerseConfigJson
	err = json.Unmarshal(resp.Config, &config)
	if err != nil {
		return nil, err
	}

	logger.Debug("Poller fetched new config",
		zap.Int("config", config.Rev),
		zap.Int("configRevEpoch", config.RevEpoch))

	parsedConfig, err := ConfigParser{}.ParseTerseConfig(&config, host)
	if err != nil {
		return nil, err
	}

	return parsedConfig, nil
}

func (w *ConfigWatcherMemd) watchThread(ctx context.Context, outCh chan<- *ParsedConfig) {
	var lastSentConfig *ParsedConfig
	var recentEndpoints []string
	allEndpointsFailed := true

	for {
		if ctx.Err() != nil {
			break
		}

		w.lock.Lock()
		state := w.state
		w.lock.Unlock()

		// if there are no endpoints to poll, we need to sleep and wait
		if len(state.endpoints) == 0 {
			select {
			case <-time.After(w.pollingPeriod):
			case <-ctx.Done():
			}

			continue
		}

		// remove the most recently polled endpoints
		var remainingEndpoints []string
		for _, endpoint := range state.endpoints {
			if !slices.Contains(recentEndpoints, endpoint) {
				remainingEndpoints = append(remainingEndpoints, endpoint)
			}
		}

		// if there are no endpoints left, we reset the lists
		if len(remainingEndpoints) == 0 {
			if allEndpointsFailed {
				// if all the endpoints failed in a row, we do a sleep to ensure
				// we don't loop for no reason
				select {
				case <-time.After(w.pollingPeriod):
				case <-ctx.Done():
				}
			}

			recentEndpoints = nil
			allEndpointsFailed = true

			continue
		}

		endpoint := remainingEndpoints[0]
		recentEndpoints = append(recentEndpoints, endpoint)

		parsedConfig, err := configWatcherMemd_pollOne(
			ctx,
			w.logger,
			w.kvClientManager,
			endpoint)
		if err != nil {
			w.logger.Debug("failed to poll config via cccp",
				zap.Error(err),
				zap.String("endpoint", endpoint))
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
		case <-time.After(w.pollingPeriod):
		case <-ctx.Done():
		}
	}

	close(outCh)
}

func (w *ConfigWatcherMemd) Watch(ctx context.Context) <-chan *ParsedConfig {
	outCh := make(chan *ParsedConfig, 1)
	go w.watchThread(ctx, outCh)
	return outCh
}
