package gocbcorex

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

type TopologyWatcherHttpConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
	BucketName       string
}

type TopologyWatcherHttpOptions struct {
	Logger *zap.Logger
}

type TopologyWatcherHttp struct {
	logger *zap.Logger

	ConfigWatcher *ConfigWatcherHttp
}

func NewTopologyWatcherHttp(configWatcher *ConfigWatcherHttp, opts *TopologyWatcherHttpOptions) (*TopologyWatcherHttp, error) {
	watcher := &TopologyWatcherHttp{
		logger: opts.Logger,

		ConfigWatcher: configWatcher,
	}

	return watcher, nil
}

func (w *TopologyWatcherHttp) Watch(ctx context.Context) <-chan *ParsedConfig {
	outCh := make(chan *ParsedConfig, 1)
	go w.watchThread(ctx, outCh)
	return outCh
}

func (w *TopologyWatcherHttp) watchThread(ctx context.Context, outCh chan<- *ParsedConfig) {
	var lastSentConfig *ParsedConfig

	// We create our own context so that we can control the shutdown flow.
	ourCtx, cancel := context.WithCancel(context.Background())
	watchCh := w.ConfigWatcher.Watch(ourCtx)

	for {
		var thisConfig *ParsedConfig
		select {
		case thisConfig = <-watchCh:
		case <-ctx.Done():
			cancel()
			// Read from the watchCh until it gets closed.
			for {
				_, ok := <-watchCh
				if !ok {
					close(outCh)
					return
				}
			}
		}

		if lastSentConfig != nil && !canUpdateConfig(thisConfig, lastSentConfig, w.logger) {
			continue
		}

		outCh <- thisConfig
		lastSentConfig = thisConfig
	}
}
