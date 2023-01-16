package core

import (
	"sync/atomic"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/stellar-nebula/core/memdx"
)

type PacketHandler func(resp *memdx.Packet, err error) bool

type KvClient interface {
	HasFeature(feat memd.HelloFeature) bool

	Dispatch(req *memdx.Packet, handler PacketHandler) error
	Close() error

	LoadFactor() float64
}

type kvClient struct {
	pendingOperations uint64
}

var _ KvClient = (*kvClient)(nil)

type kvClientOptions struct {
	hostname string
	features []memd.HelloFeature
}

func newKvClient(opts kvClientOptions) *kvClient {
	return &kvClient{}
}

func (c *kvClient) bootstrap() {
	// HELLO
	// GET_ERROR_MAP
	// AUTH (SASL_LIST_MECHS, SASL_AUTH, SASL_CONTINUE)
	// SELECT_BUCKET
	// GET_CLUSTER_CONFIG
	// OPERATIONS
}

func (c *kvClient) HasFeature(feat memd.HelloFeature) bool {
	return false
}

func (c *kvClient) Dispatch(req *memdx.Packet, handler PacketHandler) error {
	return nil
}

func (c *kvClient) Close() error {
	return nil
}

func (c *kvClient) LoadFactor() float64 {
	return (float64)(atomic.LoadUint64(&c.pendingOperations))
}
