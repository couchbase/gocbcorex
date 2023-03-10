package gocbcorex

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type kvClientPoolClientReadyFunc func(error, KvClient, KvClient)

type kvClientPoolClient struct {
	logger      *zap.Logger
	newKvClient NewKvClientFunc

	lock               sync.Mutex
	managerCancelFn    func()
	readyQueue         []kvClientPoolClientReadyFunc
	forceNewClient     bool
	latestConfig       *KvClientConfig
	reconfigureWakeSig *sync.Cond
	managerClosedSigCh chan struct{}

	// manager state
	shutdownClients []KvClient
}

type kvClientPoolClientOptions struct {
	Logger      *zap.Logger
	NewKvClient NewKvClientFunc
}

func newKvClientPoolClient(
	config *KvClientConfig,
	opts *kvClientPoolClientOptions,
	readyCb kvClientPoolClientReadyFunc,
) (*kvClientPoolClient, error) {
	if config == nil {
		return nil, errors.New("must pass config")
	}
	if opts == nil {
		opts = &kvClientPoolClientOptions{}
	}

	logger := loggerOrNop(opts.Logger)

	var newKvClient NewKvClientFunc
	if opts.NewKvClient != nil {
		newKvClient = opts.NewKvClient
	} else {
		newKvClient = func(ctx context.Context, config *KvClientConfig) (KvClient, error) {
			return NewKvClient(ctx, config, &KvClientOptions{
				Logger: logger.Named("client"),
			})
		}
	}

	managerCtx, managerCancelFn := context.WithCancel(context.Background())

	pc := &kvClientPoolClient{
		logger:      logger,
		newKvClient: newKvClient,

		managerCancelFn:    managerCancelFn,
		managerClosedSigCh: make(chan struct{}),
	}
	pc.reconfigureWakeSig = sync.NewCond(&pc.lock)

	pc.readyQueue = append(pc.readyQueue, readyCb)
	pc.latestConfig = config
	go pc.managerThread(managerCtx)

	return pc, nil
}

func (pc *kvClientPoolClient) managerThread(ctx context.Context) {
	var client KvClient

	for {
		pc.lock.Lock()

		// if we already have a valid client, we should wait for a reconfigure
		// request before we try to reconfigure the client
		if client != nil && len(pc.readyQueue) == 0 {
			pc.reconfigureWakeSig.Wait()
		}

		readyQueue := make([]kvClientPoolClientReadyFunc, len(pc.readyQueue))
		copy(readyQueue, pc.readyQueue)
		pc.readyQueue = pc.readyQueue[:0]

		forceNewClient := pc.forceNewClient
		pc.forceNewClient = false

		latestConfig := pc.latestConfig
		pc.lock.Unlock()

		if forceNewClient && client != nil {
			pc.managerShutdownClient(client)
			client = nil
		}

		if latestConfig == nil {
			break
		}

		newClient, err := pc.managerReconfigure(ctx, latestConfig, client)
		if err != nil {
			pc.logger.Warn("kvClientPoolClient failed to prepare one of it's clients",
				zap.Error(err))

			prevClient := client
			for _, readyFn := range readyQueue {
				readyFn(err, nil, prevClient)
				prevClient = nil
			}

			if client != nil {
				pc.managerShutdownClient(client)
				client = nil
			}

			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
			}

			continue
		}

		prevClient := client
		for _, readyFn := range readyQueue {
			readyFn(nil, newClient, prevClient)
			prevClient = newClient
		}

		if client != newClient {
			if client != nil {
				// if we aren't using the previous client, we need to shut it down
				pc.managerShutdownClient(client)
			}

			client = newClient
		}
	}

	// indicate to everyone that we are no longer valid, note that once the manager
	// context is closed, we are guarenteed to no longer be accepting new requests.
	prevClient := client
	for _, readyFn := range pc.readyQueue {
		readyFn(ErrShutdown, nil, prevClient)
		prevClient = nil
	}

	if client != nil {
		pc.managerShutdownClient(client)
		client = nil
	}

	close(pc.managerClosedSigCh)
}

func (pc *kvClientPoolClient) managerShutdownClient(client KvClient) {
	// close the client
	pc.lock.Lock()
	pc.shutdownClients = append(pc.shutdownClients, client)
	pc.lock.Unlock()

	go func() {
		// make this time configurable
		cancelCtx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Shutdown(cancelCtx)
		cancelFn()
		if err != nil {
			pc.logger.Warn("failed to gracefully shutdown a kv client pool client", zap.Error(err))

			err = client.Close()
			if err != nil {
				pc.logger.Warn("failed to close kv client pool client after failed graceful shutdown", zap.Error(err))
			}
		}

		// once we've shut down the client, we can remove it from the shutdown list
		pc.lock.Lock()
		clientIdx := slices.IndexFunc(pc.shutdownClients, func(oclient KvClient) bool { return oclient == client })
		if clientIdx != -1 {
			pc.shutdownClients = sliceUnorderedDelete(pc.shutdownClients, clientIdx)
		}
		pc.lock.Unlock()
	}()
}

func (pc *kvClientPoolClient) managerReconfigure(
	ctx context.Context,
	config *KvClientConfig,
	prevClient KvClient,
) (KvClient, error) {
	if prevClient != nil {
		reconfigureCh := make(chan error, 1)
		err := prevClient.Reconfigure(config, func(err error) {
			reconfigureCh <- err
		})
		if err == nil {
			err := <-reconfigureCh
			if err == nil {
				return prevClient, nil
			}

			// if a KvClient accepts a reconfigure, but it subsequently fails, this is
			// considered an error worth notifying people about
			return nil, err
		}

		// not being permitted to reconfigure a client is not considered
		// an error worth notifying people about since it is routine.
		pc.logger.Debug("kvClientPoolClient client rejected a reconfigure request",
			zap.Error(err))
	}

	client, err := pc.newKvClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (pc *kvClientPoolClient) Reconfigure(
	config *KvClientConfig,
	readyCb kvClientPoolClientReadyFunc,
) error {
	if config == nil {
		return errors.New("must pass a new config to reconfigure")
	}

	pc.lock.Lock()
	defer pc.lock.Unlock()

	copiedConfig := *config
	pc.latestConfig = &copiedConfig
	pc.readyQueue = append(pc.readyQueue, readyCb)
	pc.reconfigureWakeSig.Signal()

	return nil
}

func (pc *kvClientPoolClient) Reset(
	readyCb kvClientPoolClientReadyFunc,
) {
	pc.lock.Lock()
	defer pc.lock.Unlock()

	pc.forceNewClient = true
	pc.readyQueue = append(pc.readyQueue, readyCb)
	pc.reconfigureWakeSig.Signal()
}

func (pc *kvClientPoolClient) shutdownManager() {
	// cancel the manager context and close the reconfigure channel to
	// signal the manager to stop processing new requests
	pc.managerCancelFn()

	pc.lock.Lock()
	pc.forceNewClient = true
	pc.latestConfig = nil
	pc.reconfigureWakeSig.Signal()
	pc.lock.Unlock()

	// wait for the manager to stop
	<-pc.managerClosedSigCh
}

func (pc *kvClientPoolClient) Shutdown(ctx context.Context) error {
	pc.shutdownManager()

	pc.lock.Lock()
	defer pc.lock.Unlock()

	for len(pc.shutdownClients) > 0 {
		client := pc.shutdownClients[0]

		pc.lock.Unlock()
		err := client.Shutdown(ctx)
		pc.lock.Lock()

		if err != nil {
			return err
		}

		clientIdx := slices.IndexFunc(pc.shutdownClients, func(oclient KvClient) bool { return oclient == client })
		if clientIdx != -1 {
			pc.shutdownClients = sliceUnorderedDelete(pc.shutdownClients, clientIdx)
		}
	}

	return nil
}

func (pc *kvClientPoolClient) Close() error {
	pc.shutdownManager()

	pc.lock.Lock()
	defer pc.lock.Unlock()

	for _, client := range pc.shutdownClients {
		client.Close()
	}

	return nil
}
