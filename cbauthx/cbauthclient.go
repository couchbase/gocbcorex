package cbauthx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type CbAuthClient struct {
	logger           *zap.Logger
	heartbeatTimeout time.Duration
	livenessTimeout  time.Duration

	rpcCli      *RevRpcClient
	authChecker *AuthCheckHttp
	authCache   *AuthCheckCached

	lastCommTs     atomic.Int64
	heartbeatTimer *time.Timer

	lock        sync.Mutex
	nodeUuid    string
	clusterUuid string
	authVersion string

	initOpts    *UpdateDBExtOptions
	initSigCh   chan struct{}
	rpcCliRunCh chan error
	closeReason error
}

type CbAuthClientOptions struct {
	Logger        *zap.Logger
	HttpTransport http.RoundTripper

	NewRevRpcClient    func(context.Context, string, RevRpcHandlers, *RevRpcClientOptions) (*RevRpcClient, error)
	NewAuthCheckHttp   func(*AuthCheckHttpOptions) *AuthCheckHttp
	NewAuthCheckCached func(AuthCheck) *AuthCheckCached

	Endpoint    string
	ServiceName string
	UserAgent   string
	Username    string
	Password    string
	ClusterUuid string

	// HeartbeatInterval specifies the interval we expect to receive
	// heartbeats from ns_server at.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout specifies the period of time we are willing to
	// wait between heartbeats before marking the connection dead.
	HeartbeatTimeout time.Duration

	// LivenessTimeout specifies how long we are willing to keep using the cache
	// after we are no longer sure that the cache is still valid (how long since
	// the last revrpc message was received from ns_server).
	LivenessTimeout time.Duration
}

func NewCbAuthClient(ctx context.Context, opts *CbAuthClientOptions) (*CbAuthClient, error) {
	// We namespace the agent to improve debugging,
	logger := opts.Logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	cli := &CbAuthClient{
		logger:           logger,
		heartbeatTimeout: opts.HeartbeatTimeout,
		livenessTimeout:  opts.LivenessTimeout,
		clusterUuid:      opts.ClusterUuid,
		rpcCliRunCh:      make(chan error),
		initSigCh:        make(chan struct{}),
	}

	newRevRpcClient := NewRevRpcClient
	if opts.NewRevRpcClient != nil {
		newRevRpcClient = opts.NewRevRpcClient
	}

	rpcCli, err := newRevRpcClient(ctx,
		fmt.Sprintf("%s/auth/v1/%s?heartbeat=%d",
			opts.Endpoint,
			opts.ServiceName,
			int(opts.HeartbeatInterval/time.Second)),
		RevRpcHandlersFnsImpl{
			HeartbeatFn:   cli.rpcHeartbeat,
			UpdateDBExtFn: cli.rpcUpdateDBExt,
		},
		&RevRpcClientOptions{
			UserAgent: opts.UserAgent,
			Username:  opts.Username,
			Password:  opts.Password,
		})
	if err != nil {
		return nil, &contextualError{
			Message: "failed to connect to revrpc",
			Cause:   err,
		}
	}

	// we capture this here before starting the thread since the
	// thread will clear the value after signalling.
	initSigCh := cli.initSigCh

	go func() {
		err := rpcCli.Run()
		cli.lock.Lock()
		if cli.heartbeatTimer != nil {
			cli.heartbeatTimer.Stop()
			cli.heartbeatTimer = nil
		}
		if cli.closeReason != nil {
			err = cli.closeReason
		}
		cli.lock.Unlock()
		cli.rpcCliRunCh <- err
	}()

	select {
	case <-initSigCh:
	case err := <-cli.rpcCliRunCh:
		return nil, &contextualError{
			Message: "revrpc connection died before initialization event",
			Cause:   err,
		}
	case <-ctx.Done():
		return nil, &contextualError{
			Message: "context cancelled before initialization event",
			Cause:   ctx.Err(),
		}
	}
	initOpts := cli.initOpts

	cli.lock.Lock()
	defer cli.lock.Unlock()

	httpTransport := http.DefaultTransport
	if opts.HttpTransport != nil {
		httpTransport = opts.HttpTransport
	}

	newAuthCheckHttp := NewAuthCheckHttp
	if opts.NewAuthCheckHttp != nil {
		newAuthCheckHttp = opts.NewAuthCheckHttp
	}
	authChecker := newAuthCheckHttp(&AuthCheckHttpOptions{
		Transport:   httpTransport,
		Uri:         opts.Endpoint + initOpts.AuthCheckEndpoint,
		ClusterUuid: cli.clusterUuid,
	})

	newAuthCheckCached := NewAuthCheckCached
	if opts.NewAuthCheckCached != nil {
		newAuthCheckCached = opts.NewAuthCheckCached
	}
	authCache := newAuthCheckCached(authChecker)

	cli.rpcCli = rpcCli
	cli.authChecker = authChecker
	cli.authCache = authCache

	// we need to mark this as the current time as an immediate CheckUserPass
	// call should succeed and not trigger the connection to be destroyed due
	// to it being stale.
	cli.lastCommTs.Store(time.Now().Unix())

	// We start a timer here which will automatically close the client if the
	// heartbeat timeout is triggered.
	cli.heartbeatTimer = time.AfterFunc(cli.heartbeatTimeout, func() {
		cli.internalCloseLocked(ErrLivenessTimeout)
	})

	return cli, nil
}

func (c *CbAuthClient) refreshLivenessLocked() {
	// store the latest time for the heartbeat
	c.lastCommTs.Store(time.Now().Unix())

	// Reset our timer so we don't close the connection
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Reset(c.heartbeatTimeout)
	}
}

func (c *CbAuthClient) rpcHeartbeat(opts *HeartbeatOptions) (bool, error) {
	c.logger.Debug("received heartbeat rpc", zap.Any("opts", opts))

	c.lock.Lock()
	defer c.lock.Unlock()

	c.refreshLivenessLocked()
	return true, nil
}

func (c *CbAuthClient) rpcUpdateDBExt(opts *UpdateDBExtOptions) (bool, error) {
	c.logger.Debug("received updateDBExt rpc", zap.Any("opts", opts))

	c.lock.Lock()
	defer c.lock.Unlock()

	c.refreshLivenessLocked()

	// If the user didn't specify a cluster-uuid, use the one we get
	// initially from ns_server instead.
	if c.clusterUuid == "" {
		c.clusterUuid = opts.ClusterUUID
	}

	// If the cluster uuid we get does not match the one that was provided
	// by the application, return an error directly
	if opts.ClusterUUID != "" && c.clusterUuid != opts.ClusterUUID {
		c.internalCloseLocked(errors.New("cluster uuid did not match expected value"))
		return true, nil
	}

	// Make sure that we don't change which nodeuuid we are talking to.  This
	// can lead to accidentally cacheing values from one node, while receiving
	// invalidations from another.
	if c.nodeUuid == "" {
		c.nodeUuid = opts.NodeUUID
	}
	if c.nodeUuid != "" && c.nodeUuid != opts.NodeUUID {
		c.internalCloseLocked(errors.New("node uuid did not match expected value"))
		return true, nil
	}

	// If the auth version has changed, we need to invalidate our cache
	if c.authVersion != opts.AuthVersion {
		c.authVersion = opts.AuthVersion
		if c.authCache != nil {
			c.authCache.Invalidate()
		}
	}

	// If the client wasn't marked as initialized yet.  Lets signal that.
	if c.initOpts == nil {
		c.initOpts = opts
	}
	if c.initSigCh != nil {
		close(c.initSigCh)
		c.initSigCh = nil
	}

	return true, nil
}

func (c *CbAuthClient) Run() error {
	// Run doesn't actually do the normal run code, but rather just pipes
	// the errors out of the runThread we already started.  This is because
	// part of initializing the connection is waiting for the first update.
	return <-c.rpcCliRunCh
}

func (c *CbAuthClient) internalCloseLocked(reason error) {
	c.logger.Debug("internal close triggered", zap.Error(reason))

	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
		c.heartbeatTimer = nil
	}

	if c.closeReason == nil {
		// Record the reason we closed the client for debugging
		c.closeReason = reason
		c.rpcCli.Close()
	}
}

func (c *CbAuthClient) Close() error {
	return c.rpcCli.Close()
}

func (a *CbAuthClient) getAuthCache(ctx context.Context) (*AuthCheckCached, error) {
	lastCommTs := a.lastCommTs.Load()
	lastCommTime := time.Unix(lastCommTs, 0)
	if time.Since(lastCommTime) > a.livenessTimeout {
		return nil, ErrLivenessTimeout
	}

	// once we've passed the last comm check, we can use the cache
	return a.authCache, nil
}

func (a *CbAuthClient) CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error) {
	authCache, err := a.getAuthCache(ctx)
	if err != nil {
		return UserInfo{}, err
	}

	return authCache.CheckUserPass(ctx, username, password)
}
