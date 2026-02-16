package gocbcorex

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

type NodesWatcherHttpConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
}

type NodesWatcherHttpReconfigureConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type NodesWatcherHttpOptions struct {
	Logger               *zap.Logger
	AutoDisconnectPeriod time.Duration
}

type nodesWatcherHttpState struct {
	authenticator    Authenticator
	httpRoundTripper http.RoundTripper
	endpoints        []string
}

type NodesWatcherHttp struct {
	stateLock sync.Mutex
	state     *nodesWatcherHttpState
	logger    *zap.Logger
	userAgent string

	kvEpToServerGroup map[string]string
	lock              sync.RWMutex

	watcher   *StreamWatcherHttp[[]nodeDescriptor]
	closedSig chan struct{}
}

func NewNodesWatcherHttp(cfg NodesWatcherHttpConfig, opts NodesWatcherHttpOptions) (*NodesWatcherHttp, error) {
	logger := loggerOrNop(opts.Logger)
	watcher, err := NewStreamWatcherHttp[[]nodeDescriptor](&StreamWatcherHttpConfig{
		HttpRoundTripper: cfg.HttpRoundTripper,
		Endpoints:        cfg.Endpoints,
		UserAgent:        cfg.UserAgent,
		Authenticator:    cfg.Authenticator,
	}, &StreamWatcherHttpOptions{
		Logger:               logger.Named("nodes-stream"),
		AutoDisconnectPeriod: opts.AutoDisconnectPeriod,
	})
	if err != nil {
		return nil, err
	}

	nw := &NodesWatcherHttp{
		state: &nodesWatcherHttpState{
			authenticator:    cfg.Authenticator,
			httpRoundTripper: cfg.HttpRoundTripper,
			endpoints:        cfg.Endpoints,
		},

		userAgent: cfg.UserAgent,
		logger:    logger,

		watcher:   watcher,
		closedSig: make(chan struct{}),
	}

	return nw, nil
}

func (nw *NodesWatcherHttp) Watch() {
	ctx, cancel := context.WithCancel(context.Background())
	nodeCh := nw.watcher.Watch(ctx, streamWatcherHttp_streamNodes)

	go func() {
		<-nw.closedSig
		cancel()
	}()

	go func() {
		for nodes := range nodeCh {
			nw.lock.Lock()

			nw.kvEpToServerGroup = make(map[string]string)
			for _, node := range nodes {
				nw.kvEpToServerGroup[node.KvEndpoint] = node.ServerGroup
			}

			nw.lock.Unlock()
		}
	}()
}

func (nw *NodesWatcherHttp) Close() {
	close(nw.closedSig)
}

func (nw *NodesWatcherHttp) Reconfigure(cfg *NodesWatcherHttpReconfigureConfig) error {
	nw.logger.Debug("Reconfiguring", zap.Any("config", cfg))

	nw.stateLock.Lock()
	nw.state = &nodesWatcherHttpState{
		httpRoundTripper: cfg.HttpRoundTripper,
		endpoints:        cfg.Endpoints,
		authenticator:    cfg.Authenticator,
	}
	nw.stateLock.Unlock()

	err := nw.watcher.Reconfigure(&StreamWatcherHttpReconfigureConfig{
		HttpRoundTripper: cfg.HttpRoundTripper,
		Endpoints:        cfg.Endpoints,
		Authenticator:    cfg.Authenticator,
	})
	if err != nil {
		nw.logger.Error("failed to reconfigure config watcher", zap.Error(err))
	}

	return nil
}

func (nw *NodesWatcherHttp) serverGroupFromKvEp(endpoint string) string {
	nw.lock.RLock()
	defer nw.lock.RUnlock()
	return nw.kvEpToServerGroup[endpoint]
}
