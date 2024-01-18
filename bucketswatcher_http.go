package gocbcorex

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"

	"golang.org/x/exp/slices"

	"go.uber.org/zap"
)

type BucketsWatcherHttpConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator

	MakeAgent func(ctx context.Context, bucketName string) (*Agent, error)
}

type BucketsWatcherHttpReconfigureConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type BucketsWatcherHttpOptions struct {
	Logger               *zap.Logger
	AutoDisconnectPeriod time.Duration
}

type bucketsWatcherHttpState struct {
	authenticator    Authenticator
	httpRoundTripper http.RoundTripper
	endpoints        []string
}

type BucketsWatcherHttp struct {
	stateLock sync.Mutex
	state     *bucketsWatcherHttpState
	logger    *zap.Logger
	userAgent string
	closed    bool

	fastMap   atomic.Pointer[map[string]*Agent]
	slowMap   map[string]*Agent
	slowLock  sync.Mutex
	makeAgent func(ctx context.Context, bucketName string) (*Agent, error)

	watcher   *StreamWatcherHttp[[]bucketDescriptor]
	closedSig chan struct{}

	needsBucketLock       sync.Mutex
	needsBucketChan       chan struct{}
	lastUpdateTriggeredAt time.Time
	updateSig             chan struct{}
}

func NewBucketsWatcherHttp(cfg BucketsWatcherHttpConfig, opts BucketsWatcherHttpOptions) (*BucketsWatcherHttp, error) {
	logger := loggerOrNop(opts.Logger)

	watcher, err := NewStreamWatcherHttp[[]bucketDescriptor](&StreamWatcherHttpConfig{
		HttpRoundTripper: cfg.HttpRoundTripper,
		Endpoints:        cfg.Endpoints,
		UserAgent:        cfg.UserAgent,
		Authenticator:    cfg.Authenticator,
	}, &StreamWatcherHttpOptions{
		Logger:               logger.Named("buckets-stream"),
		AutoDisconnectPeriod: opts.AutoDisconnectPeriod,
	})
	if err != nil {
		return nil, err
	}

	bw := &BucketsWatcherHttp{
		state: &bucketsWatcherHttpState{
			authenticator:    cfg.Authenticator,
			httpRoundTripper: cfg.HttpRoundTripper,
			endpoints:        cfg.Endpoints,
		},

		makeAgent: cfg.MakeAgent,
		userAgent: cfg.UserAgent,
		logger:    logger,
		slowMap:   make(map[string]*Agent),
		watcher:   watcher,
		closedSig: make(chan struct{}),
		updateSig: make(chan struct{}, 1),
	}

	go bw.manualUpdateThread()

	return bw, nil
}

func (w *BucketsWatcherHttp) NumAgents() int {
	w.slowLock.Lock()
	defer w.slowLock.Unlock()

	return len(w.slowMap)
}

func (w *BucketsWatcherHttp) Reconfigure(cfg *BucketsWatcherHttpReconfigureConfig) error {
	w.logger.Debug("Reconfiguring", zap.Any("config", cfg))

	w.stateLock.Lock()
	w.state = &bucketsWatcherHttpState{
		httpRoundTripper: cfg.HttpRoundTripper,
		endpoints:        cfg.Endpoints,
		authenticator:    cfg.Authenticator,
	}
	w.stateLock.Unlock()

	err := w.watcher.Reconfigure(&StreamWatcherHttpReconfigureConfig{
		HttpRoundTripper: cfg.HttpRoundTripper,
		Endpoints:        cfg.Endpoints,
		Authenticator:    cfg.Authenticator,
	})
	if err != nil {
		w.logger.Error("failed to reconfigure config watcher", zap.Error(err))
	}

	return nil
}

func (bw *BucketsWatcherHttp) manualUpdateThread() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-bw.closedSig
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-bw.updateSig:
		}

		buckets, err := bw.manuallyFetchBuckets(ctx)
		if err != nil {
			bw.logger.Info("Failed to manually fetch buckets", zap.Error(err))
			continue
		}

		bw.handleBuckets(ctx, buckets)

		bw.needsBucketLock.Lock()
		if bw.needsBucketChan != nil {
			close(bw.needsBucketChan)
			bw.needsBucketChan = nil
		}
		bw.needsBucketLock.Unlock()
	}
}

func (bw *BucketsWatcherHttp) GetAgent(ctx context.Context, bucketName string) (*Agent, error) {
	getAgentMadeAt := time.Now()

	for {
		bw.stateLock.Lock()
		if bw.closed {
			bw.stateLock.Unlock()
			return nil, errors.New("buckets watcher closed")
		}
		bw.stateLock.Unlock()

		fastMap := bw.fastMap.Load()
		if fastMap != nil {
			agent, ok := (*fastMap)[bucketName]
			if ok {
				return agent, nil
			}
		}

		bw.needsBucketLock.Lock()
		var lastUpdateTriggeredAt time.Time
		if bw.needsBucketChan == nil {
			bw.needsBucketChan = make(chan struct{})
			lastUpdateTriggeredAt = time.Now()
			bw.lastUpdateTriggeredAt = lastUpdateTriggeredAt

			bw.logger.Debug("Bucket unknown, triggering manual update", zap.String("name", bucketName))

			bw.updateSig <- struct{}{}
		} else {
			bw.logger.Debug("Bucket unknown, manual update already in progress", zap.String("name", bucketName))
		}

		needsBucketChan := bw.needsBucketChan
		lastUpdateTriggeredAt = bw.lastUpdateTriggeredAt
		bw.needsBucketLock.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-needsBucketChan:
		}

		// If the fastMap is nil then just loop around and handle above.
		fastMap = bw.fastMap.Load()
		if fastMap != nil {
			// We still don't know the bucket at this point then we can say it doesn't exist.
			agent, ok := (*fastMap)[bucketName]
			if !ok {
				if lastUpdateTriggeredAt.Before(getAgentMadeAt) {
					bw.logger.Debug("Bucket still unknown, but update after request was made - retrying", zap.String("name", bucketName))
					continue
				}
				bw.logger.Debug("Bucket still unknown, erroring", zap.String("name", bucketName))

				return nil, cbmgmtx.ResourceError{
					Cause:      cbmgmtx.ErrBucketNotFound,
					BucketName: bucketName,
				}
			}

			return agent, nil
		}
	}
}

func (bw *BucketsWatcherHttp) Watch() {
	ctx, cancel := context.WithCancel(context.Background())
	bucketCh := bw.watcher.Watch(ctx, streamWatcherHttp_streamBuckets)

	go func() {
		// Cancel the context and wait for the configCh to be closed.
		<-bw.closedSig
		cancel()
	}()

	go func() {
		for buckets := range bucketCh {
			bw.handleBuckets(ctx, buckets)
		}
	}()
}

func (bw *BucketsWatcherHttp) Close() error {
	bw.stateLock.Lock()
	defer bw.stateLock.Unlock()

	bw.logger.Debug("Closing")

	bw.closed = true
	close(bw.closedSig)
	bw.fastMap.Store(nil)

	var firstErr error
	for _, agent := range bw.slowMap {
		err := agent.Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	bw.logger.Debug("Closed")

	return firstErr
}

func (bw *BucketsWatcherHttp) handleBuckets(ctx context.Context, buckets []bucketDescriptor) {
	bw.slowLock.Lock()
	defer bw.slowLock.Unlock()

	for _, bucket := range buckets {
		if _, ok := bw.slowMap[bucket.Name]; !ok {
			bw.logger.Debug("New bucket on cluster, creating agent", zap.String("name", bucket.Name))
			agent, err := bw.makeAgent(ctx, bucket.Name)
			if err != nil {
				bw.logger.Debug("Failed to create agent", zap.String("name", bucket.Name), zap.Error(err))
				continue
			}

			bw.slowMap[bucket.Name] = agent
		}
	}

	for bucket, agent := range bw.slowMap {
		if !slices.ContainsFunc(buckets, func(descriptor bucketDescriptor) bool {
			return descriptor.Name == bucket
		}) {
			bw.logger.Debug("Bucket no longer on cluster, shutting down agent", zap.String("name", bucket))
			delete(bw.slowMap, bucket)
			err := agent.Close()
			if err != nil {
				bw.logger.Debug("Failed to close agent", zap.String("name", bucket))
			}
		}
	}

	fastMap := make(map[string]*Agent, len(bw.slowMap))
	for name, agent := range bw.slowMap {
		fastMap[name] = agent
	}

	bw.fastMap.Store(&fastMap)
}

func (bw *BucketsWatcherHttp) manuallyFetchBuckets(ctx context.Context) ([]bucketDescriptor, error) {
	var recentEndpoints []string
	var latestErr error

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		bw.stateLock.Lock()
		state := bw.state
		bw.stateLock.Unlock()

		// if there are no endpoints to poll, this is unexpected
		if len(state.endpoints) == 0 {
			return nil, errors.New("watcher has no endpoints to question for bucket existence")
		}

		// remove the endpoints that we've already used
		remainingEndpoints := filterStringsOut(state.endpoints, recentEndpoints)

		// if there are no endpoints left, we must have an error so return it
		if len(remainingEndpoints) == 0 {
			return nil, latestErr
		}

		endpoint := remainingEndpoints[0]
		recentEndpoints = append(recentEndpoints, endpoint)

		buckets, err := bucketsTracker_fetchOneBuckets(
			ctx,
			bw.logger,
			state.httpRoundTripper,
			endpoint,
			bw.userAgent,
			state.authenticator)
		if err != nil {
			latestErr = err
			bw.logger.Debug("failed to fetch buckets via http",
				zap.Error(err),
				zap.String("endpoint", endpoint))
			continue
		}

		return buckets, nil
	}
}

func bucketsTracker_fetchOneBuckets(
	ctx context.Context,
	logger *zap.Logger,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
) ([]bucketDescriptor, error) {
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
		Username:  username,
		Password:  password,
	}.GetClusterConfig(ctx, &cbmgmtx.GetClusterConfigOptions{})
	if err != nil {
		return nil, err
	}

	buckets := make([]bucketDescriptor, len(resp.BucketNames))
	for i, b := range resp.BucketNames {
		buckets[i] = bucketDescriptor{
			Name: b.BucketName,
			UUID: b.UUID,
		}
	}

	return buckets, nil
}
