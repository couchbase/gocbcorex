package gocbcorex

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"

	"go.uber.org/zap"
)

type StreamWatcherHttpConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	UserAgent        string
	Authenticator    Authenticator
}

type StreamWatcherHttpReconfigureConfig struct {
	HttpRoundTripper http.RoundTripper
	Endpoints        []string
	Authenticator    Authenticator
}

type StreamWatcherHttpOptions struct {
	Logger               *zap.Logger
	AutoDisconnectPeriod time.Duration
}

type streamWatcherHttpState struct {
	httpRoundTripper http.RoundTripper
	endpoints        []string
	userAgent        string
	authenticator    Authenticator
}

type StreamWatcherHttp[StreamT any] struct {
	logger               *zap.Logger
	autoDisconnectPeriod time.Duration

	lock  sync.Mutex
	state *streamWatcherHttpState
}

func NewStreamWatcherHttp[StreamT any](Buckets *StreamWatcherHttpConfig, opts *StreamWatcherHttpOptions) (*StreamWatcherHttp[StreamT], error) {
	autoDisconnect := opts.AutoDisconnectPeriod
	if autoDisconnect == 0 {
		autoDisconnect = 25 * time.Second
	}
	return &StreamWatcherHttp[StreamT]{
		logger:               loggerOrNop(opts.Logger),
		autoDisconnectPeriod: autoDisconnect,
		state: &streamWatcherHttpState{
			httpRoundTripper: Buckets.HttpRoundTripper,
			endpoints:        Buckets.Endpoints,
			userAgent:        Buckets.UserAgent,
			authenticator:    Buckets.Authenticator,
		},
	}, nil
}

func (w *StreamWatcherHttp[StreamT]) watch(
	ctx context.Context,
	outCh chan<- StreamT,
	execFn StreamFunction[StreamT],
) {
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

		// remove the most recently watched endpoints
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

		reqCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-reqCtx.Done():
				cancel()
			case <-time.After(w.autoDisconnectPeriod):
				cancel()
			}
		}()

		err := execFn(
			reqCtx,
			w.logger,
			state.httpRoundTripper,
			endpoint,
			state.userAgent,
			state.authenticator,
			outCh)
		cancel()
		if err != nil {
			w.logger.Debug("failed to stream via http",
				zap.Error(err),
				zap.String("endpoint", endpoint))
			continue
		}

		allEndpointsFailed = false
	}

	close(outCh)
}

func (w *StreamWatcherHttp[StreamT]) Reconfigure(cfg *StreamWatcherHttpReconfigureConfig) error {
	w.lock.Lock()
	w.state = &streamWatcherHttpState{
		httpRoundTripper: cfg.HttpRoundTripper,
		endpoints:        cfg.Endpoints,
		authenticator:    cfg.Authenticator,
	}
	w.lock.Unlock()
	return nil
}

func (w *StreamWatcherHttp[StreamT]) Watch(
	ctx context.Context,
	execFn StreamFunction[StreamT],
) <-chan StreamT {
	outCh := make(chan StreamT, 1)
	go w.watch(ctx, outCh, execFn)
	return outCh
}

func streamWatcherHttp_streamBuckets(
	ctx context.Context,
	logger *zap.Logger,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
	stream chan<- []bucketDescriptor,
) error {
	host, err := getHostFromUri(endpoint)
	if err != nil {
		return err
	}

	username, password, err := authenticator.GetCredentials(ServiceTypeMgmt, host)
	if err != nil {
		return err
	}

	resp, err := cbmgmtx.Management{
		Transport: httpRoundTripper,
		UserAgent: userAgent,
		Endpoint:  endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: username,
			Password: password,
		},
	}.StreamFullClusterConfig(ctx, &cbmgmtx.StreamFullClusterConfigOptions{})
	if err != nil {
		return err
	}

	for {
		cfg, err := resp.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		}

		buckets := make([]bucketDescriptor, len(cfg.BucketNames))
		for i, bucket := range cfg.BucketNames {
			buckets[i] = bucketDescriptor{
				Name: bucket.BucketName,
				UUID: bucket.UUID,
			}
		}

		stream <- buckets
	}
}

func streamWatcherHttp_streamNodes(
	ctx context.Context,
	logger *zap.Logger,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
	stream chan<- []nodeDescriptor,
) error {
	host, err := getHostFromUri(endpoint)
	if err != nil {
		return err
	}

	username, password, err := authenticator.GetCredentials(ServiceTypeMgmt, host)
	if err != nil {
		return err
	}

	resp, err := cbmgmtx.Management{
		Transport: httpRoundTripper,
		UserAgent: userAgent,
		Endpoint:  endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: username,
			Password: password,
		},
	}.StreamFullClusterConfig(ctx, &cbmgmtx.StreamFullClusterConfigOptions{})
	if err != nil {
		return err
	}

	for {
		cfg, err := resp.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		}

		nodes := make([]nodeDescriptor, len(cfg.Nodes))
		for i, node := range cfg.Nodes {
			nodes[i] = nodeDescriptor{
				Hostname:    node.Hostname,
				ServerGroup: node.ServerGroup,
			}
		}

		stream <- nodes
	}
}
