package cbauthx

import (
	"context"
	"crypto/tls"
	"errors"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type CbAuth struct {
	logger            *zap.Logger
	httpTransport     http.RoundTripper
	newCbAuthClient   func(context.Context, *CbAuthClientOptions) (*CbAuthClient, error)
	serviceName       string
	userAgent         string
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	livenessTimeout   time.Duration
	connectTimeout    time.Duration
	minReconnectTime  time.Duration

	lock           sync.Mutex
	endpoints      []string
	username       string
	password       string
	clusterUuid    string
	cli            *CbAuthClient
	cliEndpoint    string
	cliUsername    string
	cliPassword    string
	cliClusterUuid string

	currentClient atomic.Pointer[CbAuthClient]
	connectError  error
	connectWaitCh chan struct{}

	lastConnectTime time.Time

	closed           bool
	reconfigureSigCh chan struct{}
	threadCancel     func()
}

type CbAuthConfig struct {
	// Endpoint specifies the endpoint to connect to, including scheme, host and port
	Endpoints []string

	// Username and password provide authentication to cbauth
	Username string
	Password string

	// ClusterUuid specifies the cluster-uuid we require the cluster to be using
	// in order to consider its information valid
	ClusterUuid string
}

type CbAuthOptions struct {
	// Logger is a logger used for all logging in CbAuth
	Logger *zap.Logger

	// HttpTransport specifies the transport to use for HTTP requests.
	HttpTransport http.RoundTripper

	// NewCbAuthClient specifies a function to create a CbAuthClient
	NewCbAuthClient func(context.Context, *CbAuthClientOptions) (*CbAuthClient, error)

	// ServiceName is the identifier used to identify with ns_server for revrpc
	ServiceName string

	// UserAgent is the header placed in all requests
	UserAgent string

	// HeartbeatInterval specifies how often we want to receive heartbeat
	// messages from ns_server
	HeartbeatInterval time.Duration

	// HeartbeatTimeout specifies the period of time we are willing to
	// wait between heartbeats before marking the connection dead.
	HeartbeatTimeout time.Duration

	// LivenessTimeout specifies how long we are willing to keep using the cache
	// after we are no longer sure that the cache is still valid (how long since
	// the last revrpc message was received from ns_server).
	LivenessTimeout time.Duration

	// ConnectTimeout specifies how long to wait for each attempt to connect
	// cbauth clients.  Clients will be rejected and a new connection attempted
	// if the connect timeout exceeds this time.
	ConnectTimeout time.Duration

	// MinReconnectTime specifies the minimum amount of time between attempts
	// to connect to a node.  If a connection errors and will be retried in less
	// time than this, cbauth will wait until this time before retrying.
	MinReconnectTime time.Duration
}

func NewCbAuth(ctx context.Context, config *CbAuthConfig, opts *CbAuthOptions) (*CbAuth, error) {
	if opts.LivenessTimeout <= opts.HeartbeatInterval {
		return nil, errors.New("liveness timeout must be higher than heartbeat interval")
	}

	connectTimeout := 10 * time.Second
	if opts.ConnectTimeout > 0 {
		connectTimeout = opts.ConnectTimeout
	}

	minReconnectTime := 1 * time.Second
	if opts.MinReconnectTime > 0 {
		minReconnectTime = opts.MinReconnectTime
	}

	newCbAuthClient := NewCbAuthClient
	if opts.NewCbAuthClient != nil {
		newCbAuthClient = opts.NewCbAuthClient
	}

	auth := &CbAuth{
		logger:            opts.Logger,
		httpTransport:     opts.HttpTransport,
		newCbAuthClient:   newCbAuthClient,
		serviceName:       opts.ServiceName,
		userAgent:         opts.UserAgent,
		heartbeatInterval: opts.HeartbeatInterval,
		heartbeatTimeout:  opts.HeartbeatTimeout,
		livenessTimeout:   opts.LivenessTimeout,
		connectTimeout:    connectTimeout,
		minReconnectTime:  minReconnectTime,
		endpoints:         config.Endpoints,
		username:          config.Username,
		password:          config.Password,
		clusterUuid:       config.ClusterUuid,
		reconfigureSigCh:  make(chan struct{}),
	}

	cli, endpoint, err := auth.newClient(ctx,
		auth.endpoints[:1],
		auth.username,
		auth.password,
		auth.clusterUuid)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to connect cbauth client",
			Cause:   err,
		}
	}

	auth.currentClient.Store(cli)

	threadCtx, threadCancel := context.WithCancel(context.Background())
	auth.threadCancel = threadCancel

	go auth.runThread(
		threadCtx,
		cli,
		endpoint,
		auth.username,
		auth.password,
		auth.clusterUuid)

	return auth, nil
}

// newClient creates a new client to attempt to use.  Note that this function
// is not "thread-safe" and must be invoked singularily at a time
func (a *CbAuth) newClient(
	ctx context.Context,
	endpoints []string,
	username string,
	password string,
	clusterUuid string,
) (*CbAuthClient, string, error) {
	// Do not attempt to connect to cbauth faster than allowed
	{
		lastConnectDelta := time.Since(a.lastConnectTime)
		if lastConnectDelta < a.minReconnectTime {
			select {
			case <-time.After(a.minReconnectTime - lastConnectDelta):
			case <-ctx.Done():
				return nil, "", &contextualError{
					Message: "context cancelled between reconnect attempts",
					Cause:   ctx.Err(),
				}
			}
		}

		a.lastConnectTime = time.Now()
	}

	var availableEndpoints []string
	var failedEndpoints []string
	var failedErrors []error
	for {
		availableEndpoints = availableEndpoints[:0]
		for _, address := range endpoints {
			if !slices.Contains(failedEndpoints, address) {
				availableEndpoints = append(availableEndpoints, address)
			}
		}

		if len(availableEndpoints) == 0 {
			break
		}

		// Select the primary endpoint if its one of the valid endpoints to use,
		// otherwise, randomsly select an endpoint to connect to.
		endpoint := availableEndpoints[rand.Intn(len(availableEndpoints))]

		a.logger.Debug("attempting to build new cbauth client",
			zap.String("endpoint", endpoint))

		connectCtx, cancel := context.WithTimeout(ctx, a.connectTimeout)
		cli, err := a.newCbAuthClient(connectCtx,
			&CbAuthClientOptions{
				Logger:        a.logger,
				HttpTransport: a.httpTransport,

				Endpoint:          endpoint,
				ServiceName:       a.serviceName,
				UserAgent:         a.userAgent,
				Username:          username,
				Password:          password,
				ClusterUuid:       clusterUuid,
				HeartbeatInterval: a.heartbeatInterval,
				HeartbeatTimeout:  a.heartbeatTimeout,
				LivenessTimeout:   a.livenessTimeout,
			})
		cancel()
		if err != nil {
			failedEndpoints = append(failedEndpoints, endpoint)
			failedErrors = append(failedErrors, err)

			a.logger.Warn("failed to build new cbauth client",
				zap.Error(err))
			continue
		}

		return cli, endpoint, nil
	}

	a.logger.Warn("failed to connect to all cbauth endpoints...")
	return nil, "", &MultiConnectError{
		Reasons: failedErrors,
	}
}

func (a *CbAuth) isCliValidLocked() bool {
	return slices.Contains(a.endpoints, a.cliEndpoint) &&
		a.cliUsername == a.username &&
		a.cliPassword == a.password &&
		a.cliClusterUuid == a.clusterUuid
}

func (a *CbAuth) Reconfigure(config *CbAuthConfig) error {
	a.lock.Lock()

	a.endpoints = config.Endpoints
	a.username = config.Username
	a.password = config.Password
	if config.ClusterUuid != "" {
		a.clusterUuid = config.ClusterUuid
	}

	if !a.isCliValidLocked() {
		if a.cli != nil {
			_ = a.cli.Close()
		}
	}

	a.lock.Unlock()

	return nil
}

func (a *CbAuth) runThread(
	ctx context.Context,
	initialCli *CbAuthClient,
	initialEndpoint string,
	initialUsername string,
	initialPassword string,
	initialClusterUuid string,
) {
	for {
		var newCli *CbAuthClient
		var newEndpoint string
		var newUsername string
		var newPassword string
		var newClusterUuid string
		if initialCli != nil {
			newCli = initialCli
			newEndpoint = initialEndpoint
			newUsername = initialUsername
			newPassword = initialPassword
			newClusterUuid = initialClusterUuid

			initialCli = nil
		} else {
			a.lock.Lock()

			if a.closed {
				a.lock.Unlock()
				break
			}

			endpoints := a.endpoints
			username := a.username
			password := a.password
			clusterUuid := a.clusterUuid

			// if there is no high-level cluster-uuid specified, we should use
			// the cluster uuid associated with the previous client.
			if clusterUuid == "" {
				clusterUuid = a.cliClusterUuid
			}

			a.lock.Unlock()

			a.logger.Info("new cbauth client connecting",
				zap.Strings("endpoints", endpoints),
				zap.String("clusterUuid", clusterUuid))

			cli, endpoint, err := a.newClient(
				ctx,
				endpoints,
				username,
				password,
				clusterUuid)
			if err != nil {
				a.logger.Warn("failed to reconnect to cbauth", zap.Error(err))

				a.lock.Lock()
				a.connectError = err
				connectWaitCh := a.connectWaitCh
				a.connectWaitCh = nil
				a.lock.Unlock()

				if connectWaitCh != nil {
					close(connectWaitCh)
				}

				continue
			}

			a.logger.Info("new cbauth client established")

			newCli = cli
			newEndpoint = endpoint
			newUsername = username
			newPassword = password
			newClusterUuid = clusterUuid
		}

		a.lock.Lock()

		if a.cli != nil {
			_ = a.cli.Close()
		}

		a.cli = newCli
		a.cliEndpoint = newEndpoint
		a.cliUsername = newUsername
		a.cliPassword = newPassword
		a.cliClusterUuid = newClusterUuid

		a.currentClient.Store(newCli)

		runCli := a.cli

		a.connectError = nil
		connectWaitCh := a.connectWaitCh
		a.connectWaitCh = nil

		if !a.isCliValidLocked() {
			// closed or reconfigured since initial connect
			a.logger.Debug("disconnecting new cli due to invalidity")
			_ = runCli.Close()
		}

		hasClosed := a.closed

		a.lock.Unlock()

		if connectWaitCh != nil {
			close(connectWaitCh)
		}

		if hasClosed {
			break
		}

		err := runCli.Run()
		if err != nil {
			a.logger.Warn("lost connection to cbauth", zap.Error(err))
		} else {
			a.logger.Debug("disconnected from cbauth")
		}
	}

	a.lock.Lock()
	if a.threadCancel != nil {
		a.threadCancel()
		a.threadCancel = nil
	}
	a.lock.Unlock()
}

func (a *CbAuth) CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error) {
	cli := a.currentClient.Load()
	info, err := cli.CheckUserPass(ctx, username, password)
	if err != nil {
		checkFunc := func() (UserInfo, error) {
			return a.CheckUserPass(ctx, username, password)
		}

		return a.handleAuthCheckErr(ctx, err, cli, checkFunc)
	}

	return info, nil
}

func (a *CbAuth) CheckCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
	cli := a.currentClient.Load()
	info, err := cli.CheckCertificate(ctx, connState)
	if err != nil {
		checkFunc := func() (UserInfo, error) {
			return a.CheckCertificate(ctx, connState)
		}

		return a.handleAuthCheckErr(ctx, err, cli, checkFunc)
	}

	return info, nil
}

func (a *CbAuth) handleAuthCheckErr(
	ctx context.Context,
	err error,
	cli *CbAuthClient,
	checkFn func() (UserInfo, error),
) (UserInfo, error) {
	if errors.Is(err, ErrInvalidAuth) {
		return UserInfo{}, ErrInvalidAuth
	}

	a.logger.Debug("failed to check user with cbauth", zap.Error(err))

	a.lock.Lock()

	newCli := a.currentClient.Load()
	if newCli != cli {
		// if a new client is available already, we can immediately
		// retry the request with the new client.
		a.lock.Unlock()
		return checkFn()
	}

	connectErr := a.connectError
	if connectErr != nil {
		a.lock.Unlock()
		return UserInfo{}, &contextualError{
			Message: "cannot check auth due to cbauth unavailability",
			Cause:   connectErr,
		}
	}

	if a.connectWaitCh == nil {
		a.connectWaitCh = make(chan struct{})
	}
	connectWaitCh := a.connectWaitCh

	a.lock.Unlock()

	// if the error was a liveness error, the client is never going to
	// recover, so don't bother retrying on a timer basis, we MUST wait
	// for a new client in this case.  If it was some other kind of error
	// we can try again after waiting a little bit of time.
	var timeWaitCh <-chan time.Time
	if !errors.Is(err, ErrLivenessTimeout) {
		timeWaitCh = time.After(100 * time.Millisecond)
	}

	select {
	case <-timeWaitCh:
	case <-connectWaitCh:
	case <-ctx.Done():
		return UserInfo{}, err
	}

	return checkFn()
}

func (a *CbAuth) Close() error {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.closed {
		return net.ErrClosed
	}

	a.closed = true
	if a.cli != nil {
		_ = a.cli.Close()
	}
	if a.threadCancel != nil {
		a.threadCancel()
		a.threadCancel = nil
	}

	return nil
}
