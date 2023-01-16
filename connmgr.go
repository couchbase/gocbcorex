package core

import (
	"sync"

	"golang.org/x/exp/slices"
)

type ConnExecuteHandler func(*KvClient, error) error

type ConnectionManager interface {
	UpdateEndpoints(endpoints []string) error

	Execute(endpoint string, handler ConnExecuteHandler) error
}

type connMgrPipeline struct {
	connections []*KvClient
}

type connectionManager struct {
	lock        sync.Mutex
	connections map[string]*connMgrPipeline
}

var _ ConnectionManager = (*connectionManager)(nil)

func newConnectionManager() *connectionManager {
	return &connectionManager{
		connections: make(map[string]*connMgrPipeline),
	}
}

func (m *connectionManager) UpdateEndpoints(endpoints []string) error {
	m.lock.Lock()

	for _, endpoint := range endpoints {
		if m.connections[endpoint] == nil {
			// this pipeline needs to be created...
		}
	}

	for endpoint := range m.connections {
		if !slices.Contains(endpoints, endpoint) {
			// this pipeline should be destroyed
		}
	}

	m.lock.Unlock()

	return nil
}

func (m *connectionManager) Execute(endpoint string, handler ConnExecuteHandler) error {
	return nil
}
