package gocbcorex

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type metricsWorker struct {
	endpointCh       chan string
	localKvEp        string
	srvGroup         string
	kvEpLock         sync.Mutex
	serverGroupKvEps []string
	closedSig        chan struct{}
	nodeCh           <-chan []NodeDescriptor
}

func newMetricsWorker(
	logger *zap.Logger,
	localKvEp string,
	srvGroup string,
	nodeCh <-chan []NodeDescriptor,
) *metricsWorker {
	mw := &metricsWorker{
		localKvEp:  localKvEp,
		srvGroup:   srvGroup,
		endpointCh: make(chan string, 1024),
		nodeCh:     nodeCh,
		closedSig:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-mw.closedSig
		cancel()
	}()

	go mw.watchServerGroupEndpoints(ctx)

	go mw.Start(ctx)

	return mw
}

func (mw *metricsWorker) Close() {
	close(mw.closedSig)
}

func (mw *metricsWorker) Start(ctx context.Context) {
	for {
		select {
		case ep := <-mw.endpointCh:
			mw.kvEpLock.Lock()
			srvGroupKvEps := mw.serverGroupKvEps
			mw.kvEpLock.Unlock()

			if mw.localKvEp == ep {
				localMemdRequests.Add(ctx, 1)
			} else if slices.Contains(srvGroupKvEps, ep) {
				serverGroupMemdRequests.Add(ctx, 1)
			} else {
				remoteMemdRequests.Add(ctx, 1)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (mw *metricsWorker) watchServerGroupEndpoints(ctx context.Context) {
	for {
		select {
		case nodes := <-mw.nodeCh:
			var serverGroupKvEps []string
			for _, node := range nodes {
				if node.ServerGroup == mw.srvGroup {
					serverGroupKvEps = append(serverGroupKvEps, node.KvEndpoint)
				}
			}

			mw.kvEpLock.Lock()
			mw.serverGroupKvEps = serverGroupKvEps
			mw.kvEpLock.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
