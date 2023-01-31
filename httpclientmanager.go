package core

import (
	"crypto/tls"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

type HTTPClientManager interface {
	GetClient() (HTTPClient, error)
	Reconfigure(*HTTPClientManagerConfig) error
}

type NewHTTPClientFunc func(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error)

type httpClientManagerState struct {
	tlsConfig *tls.Config
	client    HTTPClient
}

type HTTPClientManagerOptions struct {
	Logger          *zap.Logger
	NewHTTPClientFn NewHTTPClientFunc

	ConnectTimeout      time.Duration
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleTimeout         time.Duration
}

type HTTPClientManagerConfig struct {
	HTTPClientConfig

	TLSConfig *tls.Config
}

type httpClientManager struct {
	lock  sync.Mutex
	state AtomicPointer[httpClientManagerState]

	newHTTPClientFn NewHTTPClientFunc
	logger          *zap.Logger
	clientOptions   *HTTPClientOptions

	defunctClients []HTTPClient
}

func NewHTTPClientManager(config *HTTPClientManagerConfig, opts *HTTPClientManagerOptions) (*httpClientManager, error) {
	if config == nil {
		return nil, errors.New("must pass config for KvClientManager")
	}
	if opts == nil {
		opts = &HTTPClientManagerOptions{}
	}

	mgr := &httpClientManager{
		logger:          opts.Logger,
		newHTTPClientFn: opts.NewHTTPClientFn,
		clientOptions: &HTTPClientOptions{
			Logger:              opts.Logger, // TODO(chvck): This isn't right
			ConnectTimeout:      opts.ConnectTimeout,
			MaxIdleConns:        opts.MaxIdleConns,
			MaxIdleConnsPerHost: opts.MaxIdleConnsPerHost,
			IdleTimeout:         opts.IdleTimeout,
		},
	}

	mgr.state.Store(&httpClientManagerState{})

	err := mgr.Reconfigure(config)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func (mgr *httpClientManager) GetClient() (HTTPClient, error) {
	client := mgr.state.Load().client

	return client, nil
}

func (mgr *httpClientManager) Reconfigure(config *HTTPClientManagerConfig) error {
	if config == nil {
		return errors.New("invalid arguments: cant reconfigure httpClientManager to nil")
	}

	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	oldState := mgr.state.Load()
	newState := &httpClientManagerState{}

	// If the tls configs don't match then we need to build new client.
	if oldState.client != nil && oldState.tlsConfig == config.TLSConfig {
		newState.tlsConfig = oldState.tlsConfig
		newState.client = oldState.client

		err := oldState.client.Reconfigure(&HTTPClientConfig{
			Username:        config.Username,
			Password:        config.Password,
			MgmtEndpoints:   config.MgmtEndpoints,
			QueryEndpoints:  config.QueryEndpoints,
			SearchEndpoints: config.SearchEndpoints,
		})
		if err != nil {
			return err
		}

		mgr.state.Store(newState)

		return nil
	}

	cli, err := mgr.newHTTPClient(&HTTPClientConfig{
		Username:        config.Username,
		Password:        config.Password,
		MgmtEndpoints:   config.MgmtEndpoints,
		QueryEndpoints:  config.QueryEndpoints,
		SearchEndpoints: config.SearchEndpoints,
	}, &HTTPClientOptions{
		Logger:              mgr.logger,
		TLSConfig:           config.TLSConfig,
		ConnectTimeout:      mgr.clientOptions.ConnectTimeout,
		MaxIdleConns:        mgr.clientOptions.MaxIdleConns,
		MaxIdleConnsPerHost: mgr.clientOptions.MaxIdleConnsPerHost,
		IdleTimeout:         mgr.clientOptions.IdleTimeout,
	})
	if err != nil {
		return err
	}
	newState.client = cli
	newState.tlsConfig = config.TLSConfig

	mgr.state.Store(newState)

	// TODO(chvck): handle storing/closing the old client

	return nil
}

func (mgr *httpClientManager) newHTTPClient(clientConfig *HTTPClientConfig, clientOpts *HTTPClientOptions) (HTTPClient, error) {
	if mgr.newHTTPClientFn != nil {
		return mgr.newHTTPClientFn(clientConfig, clientOpts)
	}
	return NewHTTPClient(clientConfig, clientOpts)
}
