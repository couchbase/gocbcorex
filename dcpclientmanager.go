package gocbcorex

import "context"

type DcpClientManager struct {
}

func NewDcpClientManager(
	config *KvClientManagerConfig,
	opts *KvClientManagerOptions,
) (*kvClientManager, error) {
	return nil, nil
}

func (m *DcpClientManager) ShutdownClient(endpoint string, client KvClient) {

}

func (m *DcpClientManager) GetClient(ctx context.Context, endpoint string) (KvClient, error) {
	return nil, nil
}

func (m *DcpClientManager) Reconfigure(opts *KvClientManagerConfig, cb func(error)) error {
	return nil
}

func (m *DcpClientManager) GetRandomClient(ctx context.Context) (KvClient, error) {
	return nil, nil
}

func (m *DcpClientManager) Close() error {
	return nil
}
