package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	ErrPoolStillConnecting = contextualDeadline{"still waiting for a connection in the pool"}
)

type NewKvClientFunc func(context.Context, *KvClientConfig) (KvClient, error)

type KvClientPool interface {
	Reconfigure(config *KvClientPoolConfig, cb func(error)) error
	GetClient(ctx context.Context) (KvClient, error)
	ShutdownClient(client KvClient)
	Close() error
}

type KvClientPoolConfig struct {
	NumConnections uint
	ClientConfig   KvClientConfig
}

type KvClientPoolOptions struct {
	Logger      *zap.Logger
	NewKvClient NewKvClientFunc
}

type kvClientPoolFastMap struct {
	activeConnections []KvClient
}

type pendingKvClient struct {
	CancelFn   func()
	CompleteCh chan struct{}
}

type kvClientPool struct {
	logger      *zap.Logger
	newKvClient NewKvClientFunc

	clientIdx uint64
	fastMap   atomic.Pointer[kvClientPoolFastMap]

	lock       sync.Mutex
	config     KvClientPoolConfig
	connectErr error
	closeSig   chan struct{}
	closed     bool
	// activeClients contains both current and defunct clients, this allows us to continue
	// dispatching operations during a rebalance.
	activeClients   []KvClient
	pendingClients  []*pendingKvClient
	currentClients  []KvClient
	defunctClients  []KvClient
	shutdownClients []KvClient

	needClientSigCh    chan struct{}
	needNoDefunctSigCh chan struct{}
}

func NewKvClientPool(config *KvClientPoolConfig, opts *KvClientPoolOptions) (*kvClientPool, error) {
	if config == nil {
		return nil, errors.New("must pass config")
	}
	if opts == nil {
		opts = &KvClientPoolOptions{}
	}
	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("poolId", uuid.NewString()[:8]),
	)

	p := &kvClientPool{
		logger: logger,
		config: *config,

		closeSig:        make(chan struct{}),
		needClientSigCh: make(chan struct{}, 1),
	}

	var newKvClient NewKvClientFunc
	if opts.NewKvClient != nil {
		newKvClient = opts.NewKvClient
	} else {
		newKvClient = func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			return NewKvClient(ctx, config, &KvClientOptions{
				Logger:       logger.Named("client"),
				CloseHandler: p.handleClientClosed,
			})
		}
	}
	p.newKvClient = newKvClient

	logger.Debug("id assigned for " + config.ClientConfig.Address)

	// we need to lock here because checkConnectionsLocked can start goroutines
	// which potentially access the shared state...
	p.lock.Lock()
	defer p.lock.Unlock()

	p.checkConnectionsLocked()

	return p, nil
}

func (p *kvClientPool) checkConnectionsLocked() {
	p.logger.Debug("checking connections")
	numWantedClients := int(p.config.NumConnections)
	numActiveClients := len(p.currentClients)
	numDefunctClients := len(p.defunctClients)
	numPendingClients := len(p.pendingClients)
	numAvailableClients := numActiveClients + numDefunctClients

	numExcessClients := numAvailableClients - numWantedClients
	if numExcessClients > 0 {
		p.logger.Debug("trimming excess clients", zap.Int("excess", numExcessClients))
		// if we have more clients available than we want, we can shut down
		// a few of them, starting with the defunct clients...
		numClosedClients := 0

		for numExcessClients > 0 && len(p.defunctClients) > 0 {
			clientToClose := p.defunctClients[0]
			p.defunctClients = slices.Delete(p.defunctClients, 0, 1)
			p.shutdownClientLocked(clientToClose)
			numExcessClients--
			numClosedClients++
		}

		for numExcessClients > 0 && len(p.currentClients) > 0 {
			clientToClose := p.currentClients[0]
			p.currentClients = slices.Delete(p.currentClients, 0, 1)
			p.shutdownClientLocked(clientToClose)
			numExcessClients--
			numClosedClients++
		}

		if numClosedClients > 0 {
			p.logger.Debug("closed excess clients", zap.Int("closed", numClosedClients))
			p.rebuildActiveClientsLocked()
		}
	}

	numNeededClients := numWantedClients - numAvailableClients - numPendingClients
	if numNeededClients > 0 {
		p.logger.Debug("needs new clients", zap.Int("needed", numNeededClients))
		for i := 0; i < numNeededClients; i++ {
			p.startNewClientLocked()
		}
	}

	// if we have active clients, or a connection error and someone was waiting
	// for a signal about client availability, let's signal them...
	if len(p.activeClients) > 0 || p.connectErr != nil {
		if p.needClientSigCh != nil {
			close(p.needClientSigCh)
			p.needClientSigCh = nil
		}
	}

	// if we have an empty defunct client list and someone is waiting for a signal
	// lets signal them...
	if len(p.defunctClients) == 0 {
		if p.needNoDefunctSigCh != nil {
			close(p.needNoDefunctSigCh)
			p.needNoDefunctSigCh = nil
		}
	}
}

func (p *kvClientPool) startNewClientLocked() <-chan struct{} {
	// create a context to run the connection in, and a thread to watch for this
	// pool closing to cancel creating that client...
	cancelCtx, cancelFn := context.WithCancel(context.Background())

	completeCh := make(chan struct{})

	// setup the new pending connection state
	pendingClient := &pendingKvClient{
		CancelFn:   cancelFn,
		CompleteCh: completeCh,
	}
	p.addPendingClientLocked(pendingClient)

	clientConfig := p.config.ClientConfig

	// create the goroutine to actually create the client
	go func() {
		p.logger.Debug("creating new client")
		client, err := p.newKvClient(cancelCtx, &clientConfig)
		cancelFn()

		p.lock.Lock()
		defer p.lock.Unlock()

		if p.closed {
			p.logger.Debug("closed during new client creation")
			if err == nil {
				if closeErr := client.Close(); closeErr != nil {
					p.logger.Debug("failed to close client")
				}
			}
			p.removePendingClientLocked(pendingClient)

			close(completeCh)
			return
		}

		if !p.removePendingClientLocked(pendingClient) {
			// if nobody was waiting for us anymore, we just return
			close(completeCh)
			return
		}

		if err != nil {
			p.connectErr = contextualError{
				Message: "failed to async connect",
				Cause:   err,
			}
			p.checkConnectionsLocked()
			close(completeCh)
			return
		}

		for !clientConfig.Equals(&p.config.ClientConfig) {
			clientConfig = p.config.ClientConfig

			reconfigureErr := make(chan error, 1)

			p.lock.Unlock()
			err := client.Reconfigure(&clientConfig, func(error) {
				reconfigureErr <- err
			})
			p.lock.Lock()

			if err != nil {
				p.logger.Warn("failed to reconfigure a new client connection", zap.Error(err))

				p.checkConnectionsLocked()
				close(completeCh)
				return
			}

			err = <-reconfigureErr
			if err != nil {
				p.logger.Warn("failed to finalize new configuration on a new client connection", zap.Error(err))

				p.checkConnectionsLocked()
				close(completeCh)
				return
			}
		}

		p.connectErr = nil
		p.addCurrentClientLocked(client)
		p.rebuildActiveClientsLocked()
		p.checkConnectionsLocked()

		close(completeCh)
	}()

	return completeCh
}

func (p *kvClientPool) shutdownClientLocked(client KvClient) {
	p.addShutdownClientLocked(client)

	go func() {
		// for now we just ungracefully kill them...
		client.Close()

		p.lock.Lock()
		defer p.lock.Unlock()

		p.removeShutdownClientLocked(client)
	}()
}

func (p *kvClientPool) ShutdownClient(client KvClient) {
	p.logger.Debug("Shutting down kv client")
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.removeCurrentClientLocked(client) {
		if !p.removeDefunctClientLocked(client) {
			return
		}
	}

	p.shutdownClientLocked(client)
	p.rebuildActiveClientsLocked()
	p.checkConnectionsLocked()
}

func (p *kvClientPool) rebuildActiveClientsLocked() {
	p.activeClients = p.activeClients[:0]
	p.activeClients = append(p.activeClients, p.currentClients...)
	p.activeClients = append(p.activeClients, p.defunctClients...)

	// this rebuilds the fast map by simply copying all the active connections
	// from the slow data into the fast map data and storing it.
	fastMapConns := make([]KvClient, len(p.activeClients))
	copy(fastMapConns, p.activeClients)
	p.fastMap.Store(&kvClientPoolFastMap{
		activeConnections: fastMapConns,
	})
}

func (p *kvClientPool) handleClientClosed(client KvClient, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.removeCurrentClientLocked(client) {
		// If the client is no longer current anyways, we have nothing to do...
		// We can get here when we Close a client so this prevents us from taking action there too.
		return
	}

	p.rebuildActiveClientsLocked()
	p.checkConnectionsLocked()
}

func (p *kvClientPool) Reconfigure(config *KvClientPoolConfig, cb func(error)) error {
	if config == nil {
		return errors.New("invalid arguments: cant reconfigure kvClientPool to nil")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.logger.Debug("reconfiguring")

	p.config = *config

	numClientsReconfiguring := int64(len(p.currentClients))
	markClientReconfigureDone := func() {
		if (atomic.AddInt64(&numClientsReconfiguring, -1)) == 0 {
			// once we are done reconfiguring all the connections, we need to
			// wait until the list of defunct connections reaches 0.
			go func() {
				err := p.WaitUntilNoDefunctClients(context.Background())
				cb(err)
			}()
		}
	}

	clientsToReconfigure := make([]KvClient, len(p.currentClients))
	copy(clientsToReconfigure, p.currentClients)
	for _, client := range clientsToReconfigure {
		client := client

		err := client.Reconfigure(&config.ClientConfig, func(err error) {
			// this can be invoke in the same call, where we already have this locked, so
			// we push it into a goroutine to be handled instead.
			go func() {
				// if we fail to reconfigure a client, we need to throw it out.
				p.lock.Lock()
				defer p.lock.Unlock()

				if !p.removeCurrentClientLocked(client) {
					// if the client is no longer current anyways, we have nothing to do...
					return
				}

				p.addDefunctClientLocked(client)
				p.checkConnectionsLocked()
				markClientReconfigureDone()
			}()
		})
		if err != nil {
			// we can't reconfigure this client so we need to replace it
			p.removeCurrentClientLocked(client)
			p.addDefunctClientLocked(client)
			markClientReconfigureDone()
			continue
		}

		// reconfiguring is successful up until this point, so it can stay in the
		// current list of clients.  it may be moved later by the Reconfigure callback.
	}

	p.rebuildActiveClientsLocked()
	p.checkConnectionsLocked()

	return nil
}

func (p *kvClientPool) WaitUntilNoDefunctClients(ctx context.Context) error {
	p.lock.Lock()

	// we need to check if the client is closed here, otherwise we can create
	// a new signal channel that is never actioned.
	if p.closed {
		p.lock.Unlock()
		return errors.New("client pool closed with pending reconfigure")
	}

	if p.needNoDefunctSigCh == nil {
		p.needNoDefunctSigCh = make(chan struct{})
	}
	needNoDefunctSigCh := p.needNoDefunctSigCh

	p.lock.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-needNoDefunctSigCh:
		return nil
	}
}

func (p *kvClientPool) GetClient(ctx context.Context) (KvClient, error) {
	fastMap := p.fastMap.Load()
	if fastMap != nil {
		fastMapNumConns := uint64(len(fastMap.activeConnections))
		if fastMapNumConns > 0 {
			clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
			conn := fastMap.activeConnections[clientIdx%fastMapNumConns]
			return conn, nil
		}
	}

	p.logger.Debug("no client found in fast map")

	return p.getClientSlow(ctx)
}

func (p *kvClientPool) getClientSlow(ctx context.Context) (KvClient, error) {
	p.lock.Lock()

	// Check if we're already closed and if so return an error.
	if p.closed {
		p.lock.Unlock()
		return nil, illegalStateError{"kv client pool already closed"}
	}

	numConns := uint64(len(p.activeClients))
	if numConns > 0 {
		clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
		conn := p.activeClients[clientIdx%numConns]
		p.lock.Unlock()
		return conn, nil
	}

	if p.connectErr != nil {
		// if we have a connect error already, it means we are in error state
		// and should just return that error directly.
		err := p.connectErr
		p.lock.Unlock()

		p.logger.Debug("found connect error", zap.Error(err))
		return nil, err
	}

	if p.needClientSigCh == nil {
		p.needClientSigCh = make(chan struct{})
	}
	clientWaitCh := p.needClientSigCh

	p.lock.Unlock()

	select {
	case <-clientWaitCh:
	case <-ctx.Done():
		ctxErr := ctx.Err()
		p.logger.Debug("context Done triggered during get client slow", zap.Error(ctxErr))
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, ErrPoolStillConnecting
		} else {
			return nil, ctxErr
		}
	case <-p.closeSig:
		return nil, illegalStateError{"kv client pool already closed"}
	}

	return p.getClientSlow(ctx)
}

func (p *kvClientPool) Shutdown(ctx context.Context) {
}

func (p *kvClientPool) Close() error {
	p.lock.Lock()

	if p.closed {
		p.lock.Unlock()
		return nil
	}

	p.logger.Info("closing")

	p.closed = true

	// Signal to anyone watching that we're closing.
	close(p.closeSig)

	p.fastMap.Store(nil)

	// Close anyone waiting on this.
	if p.needNoDefunctSigCh != nil {
		close(p.needNoDefunctSigCh)
		p.needNoDefunctSigCh = nil
	}

	// Just close all the clients in all the lists containing alive clients.
	for _, client := range p.currentClients {
		if err := client.Close(); err != nil {
			p.logger.Debug("failed to close kv client", zap.Error(err))
		}
	}
	for _, client := range p.defunctClients {
		if err := client.Close(); err != nil {
			p.logger.Debug("failed to close kv client", zap.Error(err))
		}
	}
	p.currentClients = p.currentClients[:0]
	p.defunctClients = p.defunctClients[:0]

	// We don't need to close clients in the active list as they're a part of the current and defunct
	// lists. We can just truncate the list.
	p.activeClients = p.activeClients[:0]

	pendingWaitChs := make([]chan struct{}, 0, len(p.pendingClients))
	for _, client := range p.pendingClients {
		client.CancelFn()
		pendingWaitChs = append(pendingWaitChs, client.CompleteCh)
	}
	p.lock.Unlock()

	p.logger.Debug("waiting for pending clients to complete", zap.Int("numPendingClients", len(pendingWaitChs)))
	for _, completeCh := range pendingWaitChs {
		<-completeCh
		p.logger.Debug("a pending client has completed")
	}

	p.logger.Info("closed")
	return nil
}

func (p *kvClientPool) addPendingClientLocked(client *pendingKvClient) {
	p.pendingClients = append(p.pendingClients, client)
}
func (p *kvClientPool) removePendingClientLocked(client *pendingKvClient) bool {
	clientIdx := slices.Index(p.pendingClients, client)
	if clientIdx == -1 {
		return false
	}
	p.pendingClients = sliceUnorderedDelete(p.pendingClients, clientIdx)
	return true
}

func (p *kvClientPool) addCurrentClientLocked(client KvClient) {
	p.currentClients = append(p.currentClients, client)
}
func (p *kvClientPool) removeCurrentClientLocked(client KvClient) bool {
	clientIdx := slices.IndexFunc(p.currentClients, func(oclient KvClient) bool { return oclient == client })
	if clientIdx == -1 {
		return false
	}
	p.currentClients = slices.Delete(p.currentClients, clientIdx, clientIdx+1)
	return true
}

func (p *kvClientPool) addDefunctClientLocked(client KvClient) {
	p.defunctClients = append(p.defunctClients, client)
}
func (p *kvClientPool) removeDefunctClientLocked(client KvClient) bool {
	clientIdx := slices.IndexFunc(p.defunctClients, func(oclient KvClient) bool { return oclient == client })
	if clientIdx == -1 {
		return false
	}
	p.defunctClients = slices.Delete(p.defunctClients, clientIdx, clientIdx+1)
	return true
}

func (p *kvClientPool) addShutdownClientLocked(client KvClient) {
	p.shutdownClients = append(p.shutdownClients, client)
}
func (p *kvClientPool) removeShutdownClientLocked(client KvClient) bool {
	clientIdx := slices.IndexFunc(p.shutdownClients, func(oclient KvClient) bool { return oclient == client })
	if clientIdx == -1 {
		return false
	}
	p.shutdownClients = slices.Delete(p.shutdownClients, clientIdx, clientIdx+1)
	return true
}
