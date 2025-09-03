package gocbcorex

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type DcpEndpointClientManager interface {
	KvEndpointClientProvider

	Reconfigure(config DcpEndpointClientManagerConfig)
	Close() error
}

type dcpEndpointClientManager struct {
	bucketName   string
	closeHandler func(DcpEndpointClientManager)

	endpointClientManager KvEndpointClientManager
	isClosed              atomic.Bool
}

var _ DcpEndpointClientManager = (*dcpEndpointClientManager)(nil)

type DcpEndpointClientManagerConfigClient struct {
	Address       string
	TlsConfig     *tls.Config
	Authenticator Authenticator
}

type DcpEndpointClientManagerConfig struct {
	Clients map[string]DcpEndpointClientManagerConfigClient
}

type DcpEndpointClientManagerOptions struct {
	Logger          *zap.Logger
	NewKvClientPool NewKvClientPoolFunc

	BucketName               string
	OnDemandConnect          bool
	ConnectTimeout           time.Duration
	ConnectErrThrottlePeriod time.Duration

	CloseHandler func(DcpEndpointClientManager)
	DcpEndpointClientManagerConfig
}

func NewDcpEndpointClientManager(opts *DcpEndpointClientManagerOptions) (DcpEndpointClientManager, error) {
	ecp, err := NewKvEndpointClientManager(&KvEndpointClientManagerOptions{
		Logger:                   opts.Logger,
		NewKvClientPool:          opts.NewKvClientPool,
		NumPoolConnections:       1,
		OnDemandConnect:          opts.OnDemandConnect,
		ConnectTimeout:           opts.ConnectTimeout,
		ConnectErrThrottlePeriod: opts.ConnectErrThrottlePeriod,
		KvEndpointClientManagerConfig: KvEndpointClientManagerConfig{
			Clients: make(map[string]KvEndpointClientManagerConfigClient),
		},
	})
	if err != nil {
		return nil, err
	}

	m := &dcpEndpointClientManager{
		bucketName:            opts.BucketName,
		endpointClientManager: ecp,
	}

	m.Reconfigure(opts.DcpEndpointClientManagerConfig)

	return m, nil
}

func (m *dcpEndpointClientManager) GetEndpointClient(ctx context.Context, endpoint string) (KvClient, error) {
	return m.endpointClientManager.GetEndpointClient(ctx, endpoint)
}

func (m *dcpEndpointClientManager) Reconfigure(config DcpEndpointClientManagerConfig) {
	ecConfig := KvEndpointClientManagerConfig{
		Clients: make(map[string]KvEndpointClientManagerConfigClient, len(config.Clients)),
	}

	for addr, client := range config.Clients {
		ecConfig.Clients[addr] = KvEndpointClientManagerConfigClient{
			KvClientPoolConfig: KvClientPoolConfig{
				KvClientManagerConfig: KvClientManagerConfig{
					Address:        client.Address,
					TlsConfig:      client.TlsConfig,
					Authenticator:  client.Authenticator,
					SelectedBucket: m.bucketName,
				},
			},
		}
	}

	m.endpointClientManager.Reconfigure(ecConfig)
}

func (m *dcpEndpointClientManager) Close() error {
	if !m.isClosed.CompareAndSwap(false, true) {
		return net.ErrClosed
	}

	if m.closeHandler != nil {
		m.closeHandler(m)
	}

	return m.endpointClientManager.Close()
}
