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
	"golang.org/x/exp/slices"
)

type KvTargetTlsConfig struct {
	RootCAs            *x509.CertPool
	InsecureSkipVerify bool
	CipherSuites       []uint16
}

func (v KvTargetTlsConfig) Equals(o *KvTargetTlsConfig) bool {
	return v.RootCAs.Equal(o.RootCAs) &&
		v.InsecureSkipVerify == o.InsecureSkipVerify &&
		slices.Equal(v.CipherSuites, o.CipherSuites)
}

type KvTarget struct {
	Address   string
	TLSConfig *KvTargetTlsConfig
}

func (v KvTarget) Equals(o KvTarget) bool {
	if v.Address != o.Address {
		return false
	}

	if v.TLSConfig == nil && o.TLSConfig == nil {
		// both nil, so equal
	} else if v.TLSConfig != nil && o.TLSConfig != nil {
		if !v.TLSConfig.Equals(o.TLSConfig) {
			// both not nil, but not equal
			return false
		}

		// both not nil, and equal
	} else {
		// one is nil and the other is not, not equal
		return false
	}
	return true
}

type KvClientAuth interface {
	GetAuth(address string) (username, password string, clientCert *tls.Certificate, err error)
}

type kvClientManagerClientConfig struct {
	Target         KvTarget
	Auth           KvClientAuth
	SelectedBucket string
}

func (v kvClientManagerClientConfig) Equals(o kvClientManagerClientConfig) bool {
	return v.Target.Equals(o.Target) &&
		v.Auth == o.Auth &&
		v.SelectedBucket == o.SelectedBucket
}

type KvClientManager interface {
	KvClientProvider

	UpdateTarget(newTarget KvTarget)
	UpdateAuth(newAuth KvClientAuth)
	UpdateSelectedBucket(newBucket string)
	Close() error
}

type KvClientManagerStateChangeFn func(KvClientManager, KvClient, error)

type NewKvClientFunc func(context.Context, *KvClientOptions) (KvClient, error)

type kvClientManagerState struct {
	Err    error
	Client KvClient
}

type kvClientManager struct {
	logger                   *zap.Logger
	newKvClient              NewKvClientFunc
	connectTimeout           time.Duration
	connectErrThrottlePeriod time.Duration
	onDemandConnect          bool
	bootstrapOpts            KvClientBootstrapOptions
	stateChangeHandler       KvClientManagerStateChangeFn
	dcpHandlers              KvClientDcpEventsHandlers
	dcpOpts                  *KvClientDcpOptions

	client atomic.Pointer[kvClientManagerState]

	lock              sync.Mutex
	isBuilding        bool
	isClosed          bool
	stateChangeWaitCh chan struct{}
	buildCancelFn     func()
	buildDoneCh       chan struct{}
	currentClient     KvClient
	currentConfig     *kvClientManagerClientConfig
	desiredConfig     *kvClientManagerClientConfig
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
	BootstrapOpts            KvClientBootstrapOptions
	DcpOpts                  *KvClientDcpOptions

	Target         KvTarget
	Auth           KvClientAuth
	SelectedBucket string
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
		bootstrapOpts:            opts.BootstrapOpts,
		stateChangeHandler:       opts.StateChangeHandler,
		dcpOpts:                  opts.DcpOpts,
		dcpHandlers:              opts.DcpHandlers,
		stateChangeWaitCh:        make(chan struct{}),
		desiredConfig: &kvClientManagerClientConfig{
			Target:         opts.Target,
			Auth:           opts.Auth,
			SelectedBucket: opts.SelectedBucket,
		},
	}

	if !p.onDemandConnect {
		p.maybeBeginClientBuildLocked()
	}

	return p
}

func (p *kvClientManager) UpdateTarget(newTarget KvTarget) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.desiredConfig.Target.Equals(newTarget) {
		return
	}

	p.desiredConfig.Target = newTarget
	p.updateActiveClientLocked()
	p.rebuildFastLookupLocked()
	p.maybeBeginClientBuildLocked()
}

func (p *kvClientManager) UpdateAuth(newAuth KvClientAuth) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.desiredConfig.Auth == newAuth {
		return
	}

	p.desiredConfig.Auth = newAuth
	p.updateActiveClientLocked()
	p.rebuildFastLookupLocked()
	p.maybeBeginClientBuildLocked()
}

func (p *kvClientManager) UpdateSelectedBucket(newBucket string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.desiredConfig.SelectedBucket == newBucket {
		return
	}

	p.desiredConfig.SelectedBucket = newBucket
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

		username, password, clientCert, err := desiredConfig.Auth.GetAuth(desiredConfig.Target.Address)
		if err != nil {
			handleError(err)
			continue
		}

		var tlsConfig *tls.Config
		if desiredConfig.Target.TLSConfig != nil {
			tlsConfig = &tls.Config{
				RootCAs:            desiredConfig.Target.TLSConfig.RootCAs,
				InsecureSkipVerify: desiredConfig.Target.TLSConfig.InsecureSkipVerify,
				CipherSuites:       desiredConfig.Target.TLSConfig.CipherSuites,
			}

			if clientCert != nil {
				tlsConfig.Certificates = []tls.Certificate{*clientCert}
			}
		}

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, p.connectTimeout)
		newClient, err := p.newKvClient(timeoutCtx, &KvClientOptions{
			Logger: p.logger,

			Address:   desiredConfig.Target.Address,
			TlsConfig: tlsConfig,
			Auth: &memdx.SaslAuthAutoOptions{
				Username: username,
				Password: password,
				EnabledMechs: []memdx.AuthMechanism{
					memdx.ScramSha512AuthMechanism,
					memdx.ScramSha256AuthMechanism},
			},
			SelectedBucket: desiredConfig.SelectedBucket,
			BootstrapOpts:  p.bootstrapOpts,
			DcpOpts:        p.dcpOpts,

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
