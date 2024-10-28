package gocbcorex

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyManagerEndpoint(t *testing.T, mgr *kvClientManager, clients map[string]KvClient, endpoint, address string) {
	pool, err := mgr.GetEndpoint(endpoint)
	require.NoError(t, err)

	cli, err := pool.GetClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, clients[address], cli)
}

func TestNewKvClientManagerNilConfig(t *testing.T) {
	_, err := NewKvClientManager(nil, nil)
	assert.Error(t, err)
}

func TestNewKvClientManagerApplyConfig(t *testing.T) {
	expectedNumConns := uint(3)
	auth := &PasswordAuthenticator{
		Username: "Administator",
		Password: "password",
	}
	clientConfigs := map[string]*KvClientConfig{
		"endpoint1": {
			Address:       "10.112.234.101",
			Authenticator: auth,
		},
		"endpoint2": {
			Address:       "10.112.234.102",
			Authenticator: auth,
		},
		"endpoint3": {
			Address:       "10.112.234.103",
			Authenticator: auth,
		},
	}

	var numGetPools int
	clientPools := make(map[string]KvClient)
	mgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            clientConfigs,
	}, &KvClientManagerOptions{
		Logger: nil,
		NewKvClientProviderFn: func(clientOpts *KvClientPoolConfig) (KvClientPool, error) {
			assert.Equal(t, expectedNumConns, clientOpts.NumConnections)
			numGetPools++
			poolMock := &KvClientPoolMock{
				GetClientFunc: func(ctx context.Context) (KvClient, error) {
					cli := &KvClientMock{}
					clientPools[clientOpts.ClientConfig.Address] = cli
					return cli, nil
				},
			}

			return poolMock, nil
		},
	})
	require.NoError(t, err)

	verifyManagerEndpoint(t, mgr, clientPools, "endpoint1", "10.112.234.101")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint2", "10.112.234.102")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint3", "10.112.234.103")
}

func TestNewKvClientManagerApplyConfigNoClients(t *testing.T) {
	expectedNumConns := uint(3)

	var numGetPools int
	_, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            make(map[string]*KvClientConfig),
	}, &KvClientManagerOptions{
		Logger: nil,
		NewKvClientProviderFn: func(clientOpts *KvClientPoolConfig) (KvClientPool, error) {
			numGetPools++
			poolMock := &KvClientPoolMock{}

			return poolMock, nil
		},
	})
	require.NoError(t, err)

	assert.Zero(t, numGetPools)
}

func TestKvClientManagerReconfigureNilState(t *testing.T) {
	mgr := &kvClientManager{}
	err := mgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: 1,
		Clients:            make(map[string]*KvClientConfig),
	}, func(error) {})
	var iErr illegalStateError
	assert.ErrorAs(t, err, &iErr)
}

func TestKvClientManagerReconfigureAddsEndpoints(t *testing.T) {
	expectedNumConns := uint(3)
	auth := &PasswordAuthenticator{
		Username: "Administator",
		Password: "password",
	}
	startClientConfigs := map[string]*KvClientConfig{
		"endpoint1": {
			Address:       "10.112.234.101",
			Authenticator: auth,
		},
	}
	reconfigureClientConfigs := map[string]*KvClientConfig{
		"endpoint1": {
			Address:       "10.112.234.101",
			Authenticator: auth,
		},
		"endpoint2": {
			Address:       "10.112.234.102",
			Authenticator: auth,
		},
		"endpoint3": {
			Address:       "10.112.234.103",
			Authenticator: auth,
		},
	}

	var numGetPools int
	clientPools := make(map[string]KvClient)
	reconfiguredPools := make(map[string]struct{})
	mgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            startClientConfigs,
	}, &KvClientManagerOptions{
		Logger: nil,
		NewKvClientProviderFn: func(clientOpts *KvClientPoolConfig) (KvClientPool, error) {
			assert.Equal(t, expectedNumConns, clientOpts.NumConnections)
			numGetPools++
			poolMock := &KvClientPoolMock{
				GetClientFunc: func(ctx context.Context) (KvClient, error) {
					cli := &KvClientMock{}
					clientPools[clientOpts.ClientConfig.Address] = cli
					return cli, nil
				},
				ReconfigureFunc: func(config *KvClientPoolConfig, cb func(error)) error {
					assert.Equal(t, expectedNumConns, config.NumConnections)
					reconfiguredPools[config.ClientConfig.Address] = struct{}{}

					cb(nil)
					return nil
				},
			}

			return poolMock, nil
		},
	})
	require.NoError(t, err)

	verifyManagerEndpoint(t, mgr, clientPools, "endpoint1", "10.112.234.101")

	err = mgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            reconfigureClientConfigs,
	}, func(error) {})
	require.NoError(t, err)

	assert.Len(t, reconfiguredPools, 1)
	assert.Contains(t, reconfiguredPools, "10.112.234.101")

	verifyManagerEndpoint(t, mgr, clientPools, "endpoint1", "10.112.234.101")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint2", "10.112.234.102")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint3", "10.112.234.103")
}

func TestKvClientManagerReconfigureRemovesOldEndpoints(t *testing.T) {
	expectedNumConns := uint(3)
	auth := &PasswordAuthenticator{
		Username: "Administator",
		Password: "password",
	}
	startClientConfigs := map[string]*KvClientConfig{
		"endpoint1": {
			Address:       "10.112.234.101",
			Authenticator: auth,
		},
		"endpoint2": {
			Address:       "10.112.234.102",
			Authenticator: auth,
		},
		"endpoint3": {
			Address:       "10.112.234.103",
			Authenticator: auth,
		},
	}
	reconfigureClientConfigs := map[string]*KvClientConfig{
		"endpoint1": {
			Address:       "10.112.234.101",
			Authenticator: auth,
		},
		"endpoint2": {
			Address:       "10.112.234.102",
			Authenticator: auth,
		},
	}

	var numGetPools int
	clientPools := make(map[string]KvClient)
	reconfiguredPools := make(map[string]struct{})
	var closed int
	mgr, err := NewKvClientManager(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            startClientConfigs,
	}, &KvClientManagerOptions{
		Logger: nil,
		NewKvClientProviderFn: func(clientOpts *KvClientPoolConfig) (KvClientPool, error) {
			assert.Equal(t, expectedNumConns, clientOpts.NumConnections)
			numGetPools++
			poolMock := &KvClientPoolMock{
				GetClientFunc: func(ctx context.Context) (KvClient, error) {
					cli := &KvClientMock{}
					clientPools[clientOpts.ClientConfig.Address] = cli
					return cli, nil
				},
				ReconfigureFunc: func(config *KvClientPoolConfig, cb func(error)) error {
					assert.Equal(t, expectedNumConns, config.NumConnections)
					reconfiguredPools[config.ClientConfig.Address] = struct{}{}

					cb(nil)
					return nil
				},
				CloseFunc: func() error {
					closed++
					return nil
				},
			}

			return poolMock, nil
		},
	})
	require.NoError(t, err)

	verifyManagerEndpoint(t, mgr, clientPools, "endpoint1", "10.112.234.101")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint2", "10.112.234.102")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint3", "10.112.234.103")

	err = mgr.Reconfigure(&KvClientManagerConfig{
		NumPoolConnections: expectedNumConns,
		Clients:            reconfigureClientConfigs,
	}, func(error) {})
	require.NoError(t, err)

	assert.Len(t, reconfiguredPools, 2)
	assert.Contains(t, reconfiguredPools, "10.112.234.101")
	assert.Contains(t, reconfiguredPools, "10.112.234.102")
	assert.Equal(t, 1, closed)

	verifyManagerEndpoint(t, mgr, clientPools, "endpoint1", "10.112.234.101")
	verifyManagerEndpoint(t, mgr, clientPools, "endpoint2", "10.112.234.102")

	_, err = mgr.GetEndpoint("endpoint3")
	require.Error(t, err)
}

func TestKvClientManagerGetRandomClient(t *testing.T) {
	mockClient := &KvClientMock{}
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient, nil
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
		},
	})

	cli, err := mgr.GetRandomClient(context.Background())
	require.NoError(t, err)

	assert.Equal(t, mockClient, cli)
}

func TestKvClientManagerGetClient(t *testing.T) {
	mockClient := &KvClientMock{}
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient, nil
		},
	}
	mockClient2 := &KvClientMock{}
	mockPool2 := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient2, nil
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
			"endpoint2": {
				Pool: mockPool2,
			},
		},
	})

	cli, err := mgr.GetClient(context.Background(), "endpoint1")
	require.NoError(t, err)

	assert.Equal(t, mockClient, cli)
}

func TestKvClientManagerGetEndpoint(t *testing.T) {
	mockClient := &KvClientMock{}
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient, nil
		},
	}
	mockClient2 := &KvClientMock{}
	mockPool2 := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient2, nil
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
			"endpoint2": {
				Pool: mockPool2,
			},
		},
	})

	pool, err := mgr.GetEndpoint("endpoint1")
	require.NoError(t, err)

	assert.Equal(t, mockPool, pool)
}

func TestKvClientManagerGetRandomEndpoint(t *testing.T) {
	mockClient := &KvClientMock{}
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient, nil
		},
	}
	mockClient2 := &KvClientMock{}
	mockPool2 := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient2, nil
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
			"endpoint2": {
				Pool: mockPool2,
			},
		},
	})

	pool, err := mgr.GetRandomEndpoint()
	require.NoError(t, err)

	assert.Contains(t, []KvClientPool{mockPool, mockPool2}, pool)
}

func TestKvClientManagerShutdownClient(t *testing.T) {
	mockClient := &KvClientMock{}
	var numShutdowns int
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient, nil
		},
		ShutdownClientFunc: func(client KvClient) {
			numShutdowns++
			assert.Equal(t, mockClient, client)
		},
	}
	mockClient2 := &KvClientMock{}
	mockPool2 := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return mockClient2, nil
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
			"endpoint2": {
				Pool: mockPool2,
			},
		},
	})

	mgr.ShutdownClient("endpoint1", mockClient)

	assert.Equal(t, 1, numShutdowns)
}

func TestKvClientManagerNilStates(t *testing.T) {
	mgr := &kvClientManager{}

	type test struct {
		Fn   func() error
		Name string
	}

	tests := []test{
		{
			Fn: func() error {
				_, err := mgr.GetClient(context.Background(), "endpoint1")
				return err
			},
			Name: "GetClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetRandomClient(context.Background())
				return err
			},
			Name: "GetRandomClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetEndpoint("endpoint1")
				return err
			},
			Name: "GetEndpoint",
		},
		{
			Fn: func() error {
				_, err := mgr.GetRandomEndpoint()
				return err
			},
			Name: "GetRandomEndpoint",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn()

			var iErr illegalStateError
			assert.ErrorAs(t, err, &iErr)
		})
	}
}

func TestKvClientManagerNoPools(t *testing.T) {
	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{ClientPools: map[string]*kvClientManagerPool{}})

	type test struct {
		Fn   func() error
		Name string
	}

	tests := []test{
		{
			Fn: func() error {
				_, err := mgr.GetClient(context.Background(), "endpoint1")
				return err
			},
			Name: "GetClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetRandomClient(context.Background())
				return err
			},
			Name: "GetRandomClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetEndpoint("endpoint1")
				return err
			},
			Name: "GetEndpoint",
		},
		{
			Fn: func() error {
				_, err := mgr.GetRandomEndpoint()
				return err
			},
			Name: "GetRandomEndpoint",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn()
			assert.Error(t, err)
		})
	}
}

func TestKvClientManagerNoEndpointProvided(t *testing.T) {
	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{ClientPools: map[string]*kvClientManagerPool{}})

	type test struct {
		Fn   func() error
		Name string
	}

	tests := []test{
		{
			Fn: func() error {
				_, err := mgr.GetClient(context.Background(), "")
				return err
			},
			Name: "GetClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetEndpoint("")
				return err
			},
			Name: "GetEndpoint",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn()
			assert.Error(t, err)
		})
	}
}

func TestKvClientManagerPoolReturnError(t *testing.T) {
	expectedErr := errors.New("nowalruseshere")
	mockPool := &KvClientPoolMock{
		GetClientFunc: func(ctx context.Context) (KvClient, error) {
			return nil, expectedErr
		},
	}

	mgr := &kvClientManager{}
	mgr.state.Store(&kvClientManagerState{
		ClientPools: map[string]*kvClientManagerPool{
			"endpoint1": {
				Pool: mockPool,
			},
		},
	})

	type test struct {
		Fn   func() error
		Name string
	}

	tests := []test{
		{
			Fn: func() error {
				_, err := mgr.GetClient(context.Background(), "endpoint1")
				return err
			},
			Name: "GetClient",
		},
		{
			Fn: func() error {
				_, err := mgr.GetRandomClient(context.Background())
				return err
			},
			Name: "GetRandomClient",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn()
			assert.Equal(tt, expectedErr, err)
		})
	}
}

func TestOrchestrateMemdClient(t *testing.T) {
	ep := "endpoint1"
	expectedClient := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
	}
	mgr := &KvClientManagerMock{
		GetClientFunc: func(ctx context.Context, endpoint string) (KvClient, error) {
			assert.Equal(t, ep, endpoint)

			return expectedClient, nil
		},
	}

	res, err := OrchestrateMemdClient(context.Background(), mgr, ep, func(client KvClient) (int, error) {
		assert.Equal(t, expectedClient, client)

		return 1, nil
	})
	require.NoError(t, err)

	assert.Equal(t, 1, res)
}

func TestOrchestrateRandomMemdClient(t *testing.T) {
	expectedClient := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
	}
	mgr := &KvClientManagerMock{
		GetRandomClientFunc: func(ctx context.Context) (KvClient, error) {
			return expectedClient, nil
		},
	}

	res, err := OrchestrateRandomMemdClient(context.Background(), mgr, func(client KvClient) (int, error) {
		assert.Equal(t, expectedClient, client)

		return 1, nil
	})
	require.NoError(t, err)

	assert.Equal(t, 1, res)
}

func TestOrchestrateMemdClientGetReturnError(t *testing.T) {
	ep := "endpoint1"
	expectedErr := errors.New("somesortoferror")

	mgr := &KvClientManagerMock{
		GetClientFunc: func(ctx context.Context, endpoint string) (KvClient, error) {
			assert.Equal(t, ep, endpoint)

			return nil, expectedErr
		},
		GetRandomClientFunc: func(ctx context.Context) (KvClient, error) {
			return nil, expectedErr
		},
	}

	type test struct {
		Fn   func(func(client KvClient) (int, error)) error
		Name string
	}

	tests := []test{
		{
			Fn: func(cb func(client KvClient) (int, error)) error {
				_, err := OrchestrateMemdClient(context.Background(), mgr, ep, cb)

				return err
			},
			Name: "OrchestrateMemdClient",
		},
		{
			Fn: func(cb func(client KvClient) (int, error)) error {
				_, err := OrchestrateRandomMemdClient(context.Background(), mgr, cb)

				return err
			},
			Name: "OrchestrateRandomMemdClient",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn(func(client KvClient) (int, error) {
				return 0, errors.New("shouldnt have reached here")
			})
			assert.Equal(tt, expectedErr, err)
		})
	}
}

func TestOrchestrateMemdCallbackReturnError(t *testing.T) {
	client := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
	}
	expectedErr := errors.New("somesortoferror")

	mgr := &KvClientManagerMock{
		GetClientFunc: func(ctx context.Context, endpoint string) (KvClient, error) {
			return client, nil
		},
		GetRandomClientFunc: func(ctx context.Context) (KvClient, error) {
			return client, nil
		},
	}

	type test struct {
		Fn   func(func(client KvClient) (int, error)) error
		Name string
	}

	tests := []test{
		{
			Fn: func(cb func(client KvClient) (int, error)) error {
				_, err := OrchestrateMemdClient(context.Background(), mgr, "endpoint1", cb)

				return err
			},
			Name: "OrchestrateMemdClient",
		},
		{
			Fn: func(cb func(client KvClient) (int, error)) error {
				_, err := OrchestrateRandomMemdClient(context.Background(), mgr, cb)

				return err
			},
			Name: "OrchestrateRandomMemdClient",
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := test.Fn(func(client KvClient) (int, error) {
				return 0, expectedErr
			})
			assert.ErrorIs(tt, err, expectedErr)
		})
	}
}

func TestOrchestrateRandomMemdCallbackReturnDispatchError(t *testing.T) {
	expectedClient := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
	}
	var shutdowns int
	mgr := &KvClientManagerMock{
		GetRandomClientFunc: func(ctx context.Context) (KvClient, error) {
			return expectedClient, nil
		},
		ShutdownClientFunc: func(endpoint string, client KvClient) {
			assert.Equal(t, "", endpoint)
			assert.Equal(t, expectedClient, client)
			shutdowns++
		},
	}

	var calls int
	res, err := OrchestrateRandomMemdClient(context.Background(), mgr, func(client KvClient) (int, error) {
		assert.Equal(t, expectedClient, client)
		calls++
		if calls == 1 {
			return 0, &KvClientDispatchError{
				Cause: errors.New("something naughty"),
			}
		} else {
			return 1, nil
		}
	})
	require.NoError(t, err)

	assert.Equal(t, 1, res)
	assert.Equal(t, 1, shutdowns)
}

func TestOrchestrateMemdCallbackReturnDispatchError(t *testing.T) {
	expectedClient := &KvClientMock{
		RemoteHostnameFunc: func() string { return "hostname" },
		RemoteAddrFunc:     func() net.Addr { return &net.TCPAddr{} },
		LocalAddrFunc:      func() net.Addr { return &net.TCPAddr{} },
	}
	ep := "endpoint1"
	var shutdowns int
	mgr := &KvClientManagerMock{
		GetClientFunc: func(ctx context.Context, endpoint string) (KvClient, error) {
			assert.Equal(t, ep, endpoint)
			return expectedClient, nil
		},
		ShutdownClientFunc: func(endpoint string, client KvClient) {
			assert.Equal(t, ep, endpoint)
			assert.Equal(t, expectedClient, client)
			shutdowns++
		},
	}

	var calls int
	res, err := OrchestrateMemdClient(context.Background(), mgr, ep, func(client KvClient) (int, error) {
		assert.Equal(t, expectedClient, client)
		calls++
		if calls == 1 {
			return 0, &KvClientDispatchError{
				Cause: errors.New("something naughty"),
			}
		} else {
			return 1, nil
		}
	})
	require.NoError(t, err)

	assert.Equal(t, 1, res)
	assert.Equal(t, 1, shutdowns)
}
