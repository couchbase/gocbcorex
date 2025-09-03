package gocbcorex

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type KvTargetTlsConfig struct {
	RootCAs            *x509.CertPool
	InsecureSkipVerify bool
	CipherSuites       []uint16
}

type KvTarget struct {
	Address   string
	TLSConfig *KvTargetTlsConfig
}

type KvClientAuth struct {
	Username          string
	Password          string
	ClientCertificate *tls.Certificate
}

type KvClientManager interface {
	KvClientProvider

	Reconfigure(opts KvClientManagerConfig)
	Close() error
}

type KvClientManagerStateChangeFn func(KvClientManager, KvClient, error)

type NewKvClientFunc func(context.Context, *KvClientOptions) (KvClient, error)

type kvClientManagerState struct {
	Err    error
	Client KvClient
}

type KvClientManagerConfig struct {
	Address        string
	TlsConfig      *tls.Config
	Authenticator  Authenticator
	SelectedBucket string
	BootstrapOpts  KvClientBootstrapOptions
	DcpOpts        *KvClientDcpOptions
}

func (v KvClientManagerConfig) Equals(o KvClientManagerConfig) bool {
	return v.Address == o.Address &&
		v.TlsConfig == o.TlsConfig &&
		v.Authenticator == o.Authenticator &&
		v.SelectedBucket == o.SelectedBucket &&
		v.BootstrapOpts.Equals(o.BootstrapOpts)
}

type kvClientManager struct {
	logger                   *zap.Logger
	newKvClient              NewKvClientFunc
	connectTimeout           time.Duration
	connectErrThrottlePeriod time.Duration
	onDemandConnect          bool
	stateChangeHandler       KvClientManagerStateChangeFn
	dcpHandlers              KvClientDcpEventsHandlers

	client atomic.Pointer[kvClientManagerState]

	lock              sync.Mutex
	isBuilding        bool
	isClosed          bool
	stateChangeWaitCh chan struct{}
	buildCancelFn     func()
	buildDoneCh       chan struct{}
	currentClient     KvClient
	currentConfig     *KvClientManagerConfig
	desiredConfig     *KvClientManagerConfig
	connectErr        error
	activeClient      KvClient
}

var _ KvClientManager = (*kvClientManager)(nil)

type KvClientManagerOptions struct {
	Logger      *zap.Logger
	NewKvClient NewKvClientFunc

	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration
	StateChangeHandler       KvClientManagerStateChangeFn
	DcpHandlers              KvClientDcpEventsHandlers

	KvClientManagerConfig
}

func NewKvClientManager(opts *KvClientManagerOptions) KvClientManager {
	if opts == nil {
		opts = &KvClientManagerOptions{}
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("providerId", uuid.NewString()[:8]),
	)

	connectTimeout := opts.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}

	connectErrThrottlePeriod := opts.ConnectErrThrottlePeriod
	if connectErrThrottlePeriod == 0 {
		connectErrThrottlePeriod = 1 * time.Second
	}

	newKvClient := opts.NewKvClient
	if opts.NewKvClient == nil {
		newKvClient = NewKvClient
	}

	p := &kvClientManager{
		logger:                   logger,
		newKvClient:              newKvClient,
		connectTimeout:           connectTimeout,
		connectErrThrottlePeriod: connectErrThrottlePeriod,
		onDemandConnect:          opts.OnDemandConnect,
		stateChangeHandler:       opts.StateChangeHandler,
		stateChangeWaitCh:        make(chan struct{}),
		desiredConfig:            &opts.KvClientManagerConfig,
		dcpHandlers:              opts.DcpHandlers,
	}

	if !p.onDemandConnect {
		p.maybeBeginClientBuildLocked()
	}

	return p
}

func (p *kvClientManager) Reconfigure(newConfig KvClientManagerConfig) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.desiredConfig.Equals(newConfig) {
		return
	}

	p.desiredConfig = &newConfig
	p.updateActiveClientLocked()
	p.rebuildFastLookupLocked()
	p.maybeBeginClientBuildLocked()
}

func (p *kvClientManager) updateActiveClientLocked() {
	// if there is no current client, we obviously can't use it
	if p.currentClient == nil {
		p.activeClient = nil
		return
	}

	// if we previously had no bucket selected, but now do, we cannot use
	// the client to prevent a race condition where the bucket is not
	// actually selected yet.  Note that the converse is not true, if we
	// had a bucket selected, but now do not, that is safe to use still.
	if p.currentConfig.SelectedBucket == "" && p.desiredConfig.SelectedBucket != "" {
		p.activeClient = nil
		return
	}

	p.activeClient = p.currentClient
}

func (p *kvClientManager) rebuildFastLookupLocked() {
	p.client.Store(&kvClientManagerState{
		Err:    p.connectErr,
		Client: p.activeClient,
	})
}

func (p *kvClientManager) maybeBeginClientBuildLocked() {
	if p.isClosed {
		return
	}
	if p.isBuilding {
		return
	}

	if p.activeClient != nil &&
		p.currentConfig.Equals(*p.currentConfig) {
		// already have a client with the desired config
		return
	}

	buildCtx, buildCancelFn := context.WithCancel(context.Background())
	buildDoneCh := make(chan struct{}, 1)

	p.isBuilding = true
	p.buildCancelFn = buildCancelFn
	p.buildDoneCh = buildDoneCh

	go func() {
		p.clientBuildThread(buildCtx)

		p.lock.Lock()
		p.isBuilding = false
		p.buildCancelFn = nil
		p.buildDoneCh = nil
		p.lock.Unlock()

		buildCancelFn()
		close(buildDoneCh)
	}()
}

func (p *kvClientManager) clientBuildThread(
	ctx context.Context,
) {
	p.logger.Info("client build thread started")

	lastErrTime := time.Time{}

	// we are the only writer to these values, so it is safe to read them
	// without a lock, and we do not need to refresh them while we are working.
	currentConfig := p.currentConfig
	currentClient := p.currentClient

	p.lock.Lock()
	desiredConfig := p.desiredConfig
	p.lock.Unlock()

ClientBuildLoop:
	for {
		if desiredConfig == nil {
			p.logger.DPanic("desired config is nil in client build thread")
		}
		if currentClient != nil && currentConfig == nil {
			p.logger.DPanic("current client is non-nil but current config is nil in client build thread")
		}

		if ctx.Err() != nil {
			p.logger.Debug("client build thread exiting due to context done", zap.Error(ctx.Err()))
			return
		}

		if currentConfig != nil && desiredConfig.Equals(*currentConfig) {
			// we have reached the desired config
			break
		}

		if currentClient != nil {
			bucketChangeConfig := *currentConfig
			bucketChangeConfig.SelectedBucket = desiredConfig.SelectedBucket
			if desiredConfig.Equals(bucketChangeConfig) {
				// if changing the bucket is enough to match the desired config, do that

				err := currentClient.SelectBucket(ctx, desiredConfig.SelectedBucket)
				if err == nil {
					currentConfig = desiredConfig

					p.lock.Lock()
					p.currentConfig = currentConfig
					p.updateActiveClientLocked()
					p.rebuildFastLookupLocked()
					p.lock.Unlock()
					continue
				} else {
					p.logger.Warn("failed to reconfigure existing kv client", zap.Error(err))
				}
			}
		}

		p.logger.Info("creating new client kv client",
			zap.Any("config", desiredConfig))

		for {
			connectWaitPeriod := p.connectErrThrottlePeriod - time.Since(lastErrTime)
			if connectWaitPeriod > 0 {
				p.logger.Debug("throttling client connection due to recent error",
					zap.Duration("throttlePeriod", p.connectErrThrottlePeriod),
					zap.Duration("waitPeriod", connectWaitPeriod))

				select {
				case <-ctx.Done():
					break ClientBuildLoop
				case <-time.After(connectWaitPeriod):
				}

				continue
			}

			break
		}

		handleError := func(err error) {
			if !errors.Is(err, context.Canceled) {
				p.logger.Warn("failed to create new kv client", zap.Error(err))
			}

			p.lock.Lock()
			p.connectErr = err
			p.rebuildFastLookupLocked()
			p.sendStateChangeLocked()
			desiredConfig = p.desiredConfig
			p.lock.Unlock()

			lastErrTime = time.Now()
		}

		username, password, err := desiredConfig.Authenticator.GetCredentials(
			ServiceTypeMemd, desiredConfig.Address)
		if err != nil {
			handleError(err)
			continue
		}

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, p.connectTimeout)
		newClient, err := p.newKvClient(timeoutCtx, &KvClientOptions{
			Logger: p.logger,

			Address:   desiredConfig.Address,
			TlsConfig: desiredConfig.TlsConfig,
			Auth: &memdx.SaslAuthAutoOptions{
				Username: username,
				Password: password,
				EnabledMechs: []memdx.AuthMechanism{
					memdx.ScramSha512AuthMechanism,
					memdx.ScramSha256AuthMechanism},
			},
			SelectedBucket: desiredConfig.SelectedBucket,
			BootstrapOpts:  desiredConfig.BootstrapOpts,
			DcpOpts:        desiredConfig.DcpOpts,

			DcpHandlers:  p.dcpHandlers,
			CloseHandler: p.handleClientClosed,
		})
		timeoutCancel()

		if err != nil {
			handleError(err)
			continue
		}

		existingClient := currentClient
		currentClient = newClient
		currentConfig = desiredConfig

		p.lock.Lock()
		p.currentClient = currentClient
		p.currentConfig = currentConfig
		p.connectErr = nil
		p.updateActiveClientLocked()
		p.rebuildFastLookupLocked()
		p.sendStateChangeLocked()
		desiredConfig = p.desiredConfig
		p.lock.Unlock()

		if existingClient != nil {
			err = existingClient.Close()
			if err != nil {
				p.logger.Warn("failed to close old kv client", zap.Error(err))
			}
		}
	}
}

func (p *kvClientManager) sendStateChangeLocked() {
	if p.stateChangeHandler != nil {
		p.stateChangeHandler(p, p.activeClient, p.connectErr)
	}

	// state channel is basically a constant event feed
	close(p.stateChangeWaitCh)
	p.stateChangeWaitCh = make(chan struct{})
}

func (p *kvClientManager) handleClientClosed(client KvClient, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if client != p.currentClient {
		return
	}

	p.currentConfig = nil
	p.currentClient = nil
	p.activeClient = nil

	if !p.onDemandConnect {
		p.maybeBeginClientBuildLocked()
	}

	p.rebuildFastLookupLocked()
	p.sendStateChangeLocked()
}

// GetClient will wait until a client is available to use, or an explicit
// failure to connect to the client is known (in which case, the error is returned).
func (p *kvClientManager) GetClient(ctx context.Context) (KvClient, error) {
	wrapper := p.client.Load()
	if wrapper != nil {
		if wrapper.Client != nil {
			return wrapper.Client, nil
		} else if wrapper.Err != nil {
			return nil, wrapper.Err
		}
	}

	for {
		p.lock.Lock()
		isClosed := p.isClosed
		client := p.activeClient
		connectErr := p.connectErr
		p.maybeBeginClientBuildLocked()
		waitCh := p.stateChangeWaitCh
		p.lock.Unlock()

		if isClosed {
			return nil, errors.New("client provider is closed")
		}
		if client != nil {
			return client, nil
		}
		if connectErr != nil {
			return nil, connectErr
		}

		select {
		case <-waitCh:
			// continue
		case <-ctx.Done():
			ctxErr := ctx.Err()
			p.logger.Debug("context Done triggered during get client slow", zap.Error(ctxErr))
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return nil, ErrClientStillConnecting
			} else {
				return nil, ctxErr
			}
		}

	}
}

func (p *kvClientManager) Close() error {
	p.logger.Debug("closing kv client manager")

	p.lock.Lock()

	p.isClosed = true
	p.activeClient = nil
	isBuilding := p.isBuilding
	buildCancelFn := p.buildCancelFn
	buildDoneCh := p.buildDoneCh

	if isBuilding {
		p.lock.Unlock()

		if buildCancelFn == nil || buildDoneCh == nil {
			p.logger.DPanic("inconsistent state in kv client manager close")
		}

		buildCancelFn()
		<-buildDoneCh

		p.lock.Lock()
	}

	currentClient := p.currentClient
	p.currentConfig = nil
	p.currentClient = nil

	p.lock.Unlock()

	if currentClient != nil {
		closeErr := currentClient.Close()
		if closeErr != nil {
			return closeErr
		}
	}

	return nil
}
