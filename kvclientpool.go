package core

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/slices"
)

var (
	ErrPoolStillConnecting = contextualDeadline{"still waiting for a connection in the pool"}
)

type ConnectionProvider interface {
	GetClient(ctx context.Context) (KvClient, error)
	ShutdownClient(client KvClient)
}

type KvClientPoolOptions struct {
	NewKvClient    func(context.Context, *KvClientOptions) (KvClient, error)
	NumConnections uint
	ClientOpts     KvClientOptions
}

type kvClientPoolFastMap struct {
	activeConnections []KvClient
}

type pendingConnectionState struct {
	NewKvClient func(context.Context, *KvClientOptions) (KvClient, error)
	ClientOpts  KvClientOptions

	IsReady bool
	Err     error
	Client  KvClient
}

type KvClientPool struct {
	config KvClientPoolOptions

	clientIdx uint64
	fastMap   AtomicPointer[kvClientPoolFastMap]

	lock                sync.Mutex
	connectErr          error
	pendingConnection   *pendingConnectionState
	reconfigConnections []KvClient
	activeConnections   []KvClient
	defunctConnections  []KvClient

	wakeManagerSigCh chan struct{}
	needClientSigCh  chan struct{}
	closeSigCh       chan struct{}
}

func NewKvClientPool(opts *KvClientPoolOptions) (*KvClientPool, error) {
	if opts == nil {
		return nil, errors.New("must pass options")
	}

	config := *opts

	if config.NewKvClient == nil {
		config.NewKvClient = func(ctx context.Context, opts *KvClientOptions) (KvClient, error) {
			return newKvClient(ctx, opts)
		}
	}

	p := &KvClientPool{
		config: config,

		wakeManagerSigCh: nil, // manager defaults to 'awake'
		needClientSigCh:  make(chan struct{}, 1),
		closeSigCh:       make(chan struct{}, 1),
	}
	go p.managerThread()

	return p, nil
}

func (p *KvClientPool) sleepManagerLocked() <-chan struct{} {
	if p.wakeManagerSigCh == nil {
		p.wakeManagerSigCh = make(chan struct{}, 1)
	}
	return p.wakeManagerSigCh
}

func (p *KvClientPool) wakeManagerLocked() {
	if p.wakeManagerSigCh == nil {
		// manager is already awake
		return
	}

	wakeManagerSigCh := p.wakeManagerSigCh
	p.wakeManagerSigCh = nil

	if wakeManagerSigCh != nil {
		close(wakeManagerSigCh)
	}
}

func (p *KvClientPool) sleepClientWaiterLocked() (<-chan struct{}, error) {
	if p.needClientSigCh == nil {
		return nil, errors.New("illegal state: client sleep wait channel")
	}

	return p.needClientSigCh, nil
}

func (p *KvClientPool) notifyClientWaitersLocked() {
	needClientSigCh := p.needClientSigCh
	p.needClientSigCh = nil

	if needClientSigCh != nil {
		close(needClientSigCh)
	}
}

func (p *KvClientPool) startPendingConnectionLocked() {
	if p.pendingConnection != nil {
		return
	}

	// setup the new pending connection state
	pendingConn := &pendingConnectionState{
		NewKvClient: p.config.NewKvClient,
		ClientOpts:  p.config.ClientOpts,
	}
	p.pendingConnection = pendingConn

	// create a context to run the connection in, and a thread to watch for this
	// pool closing to cancel creating that client...
	cancelCtx, cancelFn := context.WithCancel(context.Background())
	go func() {
		select {
		case <-p.closeSigCh:
			cancelFn()
			<-cancelCtx.Done()
		case <-cancelCtx.Done():
		}
	}()

	// create the goroutine to actually create the client
	go func() {
		client, err := pendingConn.NewKvClient(cancelCtx, &pendingConn.ClientOpts)
		cancelFn()

		p.lock.Lock()
		p.pendingConnection.IsReady = true
		p.pendingConnection.Client = client
		p.pendingConnection.Err = err
		p.wakeManagerLocked()
		p.lock.Unlock()
	}()
}

func (p *KvClientPool) checkPendingConnectionLocked() {
	if p.pendingConnection == nil {
		return
	}
	if !p.pendingConnection.IsReady {
		return
	}

	pendingConn := p.pendingConnection
	p.pendingConnection = nil

	newClient, err := pendingConn.Client, pendingConn.Err
	if err != nil {
		log.Printf("async connect failed: %s", err)

		p.connectErr = err
		p.notifyClientWaitersLocked()
		return
	}

	// clear our connect error after a succesful connect
	p.connectErr = nil

	// if the client options changed since we started the connect, we need to perform
	// a reconfiguring of that new client before we can use it...
	if !pendingConn.ClientOpts.Equals(&p.config.ClientOpts) {
		p.reconfigConnections = append(p.reconfigConnections, newClient)
		go p.reconfigureClientThread(newClient)
		return
	}

	p.activeConnections = append(p.activeConnections, newClient)
	p.rebuildFastMapLocked()
	p.notifyClientWaitersLocked()
}

func (p *KvClientPool) checkNumConnectionsLocked() {
	numWantedConns := int(p.config.NumConnections)
	numActiveConns := len(p.activeConnections)
	numReconfigureConns := len(p.reconfigConnections)

	numTotalConns := 0
	numTotalConns += numActiveConns
	numTotalConns += numReconfigureConns
	if p.pendingConnection != nil {
		numTotalConns++
	}

	if numTotalConns < numWantedConns {
		p.startPendingConnectionLocked()
	}

	if numActiveConns > numWantedConns {
		defunctConns := p.activeConnections[numWantedConns:]
		p.activeConnections = p.activeConnections[:numWantedConns]
		p.rebuildFastMapLocked()

		for _, conn := range defunctConns {
			go p.defunctClientThread(conn)
		}
	}
}

func (p *KvClientPool) managerThread() {
ManagerLoop:
	for {
		p.lock.Lock()

		// check if we have a pending connection ready to add to the pool
		p.checkPendingConnectionLocked()

		// check if we need to start adding or removing connections
		p.checkNumConnectionsLocked()

		// grab the wake channel so we can sleep until there is actually something
		// for us to be doing...
		wakeManagerCh := p.sleepManagerLocked()

		p.lock.Unlock()

		select {
		case <-wakeManagerCh:
		case <-p.closeSigCh:
			break ManagerLoop
		}
	}
}

func (p *KvClientPool) reconfigureClientThread(client KvClient) {
	cancelCtx, cancelFn := context.WithCancel(context.Background())
	go func() {
		select {
		case <-p.closeSigCh:
			cancelFn()
			<-cancelCtx.Done()
		case <-cancelCtx.Done():

		}
	}()

	p.lock.Lock()
	reconfigureOpts := p.config.ClientOpts
	p.lock.Unlock()

	for {
		err := client.Reconfigure(cancelCtx, &reconfigureOpts)
		if err != nil {
			cancelFn()

			// if we failed to reconfigure the client, we just remove it from the reconfiguring
			// list and then wake the manager to start building a new connection
			p.lock.Lock()

			clientIdx := slices.IndexFunc(p.activeConnections, func(oclient KvClient) bool { return oclient == client })
			if clientIdx >= 0 {
				p.reconfigConnections[clientIdx] = p.reconfigConnections[len(p.reconfigConnections)-1]
				p.reconfigConnections = p.reconfigConnections[:len(p.reconfigConnections)-1]
			}

			p.wakeManagerLocked()

			p.lock.Unlock()
			return
		}

		p.lock.Lock()
		if !reconfigureOpts.Equals(&p.config.ClientOpts) {
			// the config options have changed, we need to try again
			reconfigureOpts = p.config.ClientOpts
			continue
		}

		clientIdx := slices.IndexFunc(p.reconfigConnections, func(oclient KvClient) bool { return oclient == client })
		if clientIdx == -1 {
			// we've successfully reconfigured a connection that should not be getting reconfigured...
			panic("reconfigure complete of unreconfiguring client")
		}

		// remove it from the reconfiguring list and add it to the configured list
		p.reconfigConnections[clientIdx] = p.reconfigConnections[len(p.reconfigConnections)-1]
		p.reconfigConnections = p.reconfigConnections[:len(p.reconfigConnections)-1]
		p.activeConnections = append(p.activeConnections, client)

		p.rebuildFastMapLocked()
		p.notifyClientWaitersLocked()

		p.lock.Unlock()
		return
	}
}

func (p *KvClientPool) defunctClientThread(client KvClient) {
	// TODO(brett19): Actually shut down defunct clients...
}

func (p *KvClientPool) ShutdownClient(client KvClient) {
	p.lock.Lock()

	// find the index in our active connections
	clientIdx := slices.IndexFunc(p.activeConnections, func(oclient KvClient) bool { return oclient == client })
	if clientIdx == -1 {
		p.lock.Unlock()
		return
	}

	// remove the active client
	defunctClient := p.activeConnections[clientIdx]
	p.activeConnections[clientIdx] = p.activeConnections[len(p.activeConnections)-1]
	p.activeConnections = p.activeConnections[:len(p.activeConnections)-1]
	p.rebuildFastMapLocked()

	// add it to defunct ones
	p.defunctConnections = append(p.defunctConnections, defunctClient)
	go p.defunctClientThread(defunctClient)

	if len(p.activeConnections) == 0 {
		// because we've invalidated all the clients, we need a new needsClientCh
		p.needClientSigCh = make(chan struct{}, 1)
	}

	// wake the manager so it can build new connections
	p.wakeManagerLocked()

	p.lock.Unlock()

}

func (p *KvClientPool) rebuildFastMapLocked() {
	// this function rebuilds the fast map by simply copying all the active
	// connections from the slow data into the fast map data and storing it.

	fastMapConns := make([]KvClient, len(p.activeConnections))
	copy(fastMapConns, p.activeConnections)
	p.fastMap.Store(&kvClientPoolFastMap{
		activeConnections: fastMapConns,
	})
}

func (p *KvClientPool) Reconfigure(opts *KvClientPoolOptions) error {
	if opts == nil {
		return errors.New("you must pass options")
	}
	// there are no options that make reconfiguring fail

	p.lock.Lock()
	p.config = *opts

	// move all the active connections to reconfiguring
	reconfigureClients := p.activeConnections
	p.activeConnections = p.activeConnections[:0]
	p.reconfigConnections = append(p.reconfigConnections, reconfigureClients...)

	// start the reconfiguring
	for _, client := range reconfigureClients {
		go p.reconfigureClientThread(client)
	}

	// because we've invalidated all the clients, we need a new needsClientCh
	p.needClientSigCh = make(chan struct{}, 1)

	// we shouldn't need to wake the manager, since all the connections are valid, just
	// being reconfigured, but we do it for consistency.
	p.wakeManagerLocked()

	p.lock.Unlock()
	return nil
}

func (p *KvClientPool) GetClient(ctx context.Context) (KvClient, error) {
	fastMap := p.fastMap.Load()
	if fastMap != nil {
		fastMapNumConns := uint64(len(fastMap.activeConnections))
		if fastMapNumConns > 0 {
			clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
			conn := fastMap.activeConnections[clientIdx%fastMapNumConns]
			return conn, nil
		}
	}

	return p.getClientSlow(ctx)
}

func (p *KvClientPool) getClientSlow(ctx context.Context) (KvClient, error) {
	p.lock.Lock()

	numConns := uint64(len(p.activeConnections))
	if numConns > 0 {
		clientIdx := atomic.AddUint64(&p.clientIdx, 1) - 1
		conn := p.activeConnections[clientIdx%numConns]
		p.lock.Unlock()
		return conn, nil
	}

	if p.connectErr != nil {
		// if we have a connect error already, it means we are in error state
		// and should just return that error directly.
		p.lock.Unlock()
		return nil, p.connectErr
	}

	clientWaitCh, err := p.sleepClientWaiterLocked()
	if err != nil {
		p.lock.Unlock()
		return nil, err
	}

	p.lock.Unlock()

	select {
	case <-clientWaitCh:
	case <-ctx.Done():
		ctxErr := ctx.Err()
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, ErrPoolStillConnecting
		} else {
			return nil, ctxErr
		}
	}

	return p.getClientSlow(ctx)
}

func (p *KvClientPool) Shutdown(ctx context.Context) {
	// TODO(brett19): Implement graceful shutdown of a pool...
}

func (p *KvClientPool) Close() {
	// TODO(brett19): Implementing closing a client pool
}
