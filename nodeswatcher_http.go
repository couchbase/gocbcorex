package gocbcorex

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
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

	hostnameToServerGroup map[string]string
	lock                  sync.Mutex

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

			nw.hostnameToServerGroup = make(map[string]string)
			for _, node := range nodes {
				nw.hostnameToServerGroup[node.Hostname] = node.ServerGroup
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

func (nw *NodesWatcherHttp) GetNodes(ctx context.Context) map[string]string {
	nw.lock.Lock()
	defer nw.lock.Unlock()
	if nw.hostnameToServerGroup == nil {
		nodes, err := nw.fetchNodes(
			ctx,
			nw.state.httpRoundTripper,
			nw.state.endpoints[0],
			nw.userAgent,
			nw.state.authenticator,
		)
		if err != nil {
			nw.logger.Error("failed to fetch nodes", zap.Error(err))
			return nil
		}

		nw.hostnameToServerGroup = make(map[string]string)
		for _, node := range nodes {
			nw.hostnameToServerGroup[node.Hostname] = node.ServerGroup
		}
	}

	return nw.hostnameToServerGroup
}

func (nw *NodesWatcherHttp) fetchNodes(
	ctx context.Context,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
) ([]nodeDescriptor, error) {
	host, err := getHostFromUri(endpoint)
	if err != nil {
		return nil, err
	}

	username, password, err := authenticator.GetCredentials(ServiceTypeMgmt, host)
	if err != nil {
		return nil, err
	}

	resp, err := cbmgmtx.Management{
		Transport: httpRoundTripper,
		UserAgent: userAgent,
		Endpoint:  endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: username,
			Password: password,
		},
	}.GetClusterConfig(ctx, &cbmgmtx.GetClusterConfigOptions{})
	if err != nil {
		return nil, err
	}

	nodes := make([]nodeDescriptor, len(resp.Nodes))
	for i, n := range resp.Nodes {
		nodes[i] = nodeDescriptor{
			Hostname:    n.Hostname,
			ServerGroup: n.ServerGroup,
		}
	}

	return nodes, nil
}
