package core

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"sync/atomic"

	"golang.org/x/exp/slices"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type KvClientOptions struct {
	Address        string
	TlsConfig      *tls.Config
	SelectedBucket string
	Username       string
	Password       string
}

func (o KvClientOptions) Equals(b *KvClientOptions) bool {
	return o.Address == b.Address &&
		o.TlsConfig == b.TlsConfig &&
		o.SelectedBucket == b.SelectedBucket &&
		o.Username == b.Username &&
		o.Password == b.Password
}

type KvClient interface {
	Reconfigure(ctx context.Context, opts *KvClientOptions) error

	GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error)
	GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) ([]byte, error)
	Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error)
	Set(ctx context.Context, req *memdx.SetRequest) (*memdx.SetResponse, error)

	Close() error

	LoadFactor() float64
}

type kvClient struct {
	pendingOperations uint64
	cli               *memdx.Client

	hostname  string
	tlsConfig *tls.Config
	bucket    string

	username string
	password string

	requestedFeatures []memdx.HelloFeature
	supportedFeatures []memdx.HelloFeature
}

var _ KvClient = (*kvClient)(nil)

func newKvClient(ctx context.Context, opts *KvClientOptions) (*kvClient, error) {
	conn, err := memdx.DialConn(ctx, opts.Address, &memdx.DialConnOptions{TLSConfig: opts.TlsConfig})
	if err != nil {
		return nil, err
	}

	cli := memdx.NewClient(conn, &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	kvCli := &kvClient{
		cli:               cli,
		hostname:          opts.Address,
		tlsConfig:         opts.TlsConfig,
		requestedFeatures: nil, // TODO(brett19): fix this
		username:          opts.Username,
		password:          opts.Password,
		bucket:            opts.SelectedBucket,
	}

	res, err := kvCli.bootstrap(ctx, &memdx.BootstrapOptions{
		Hello: &memdx.HelloRequest{
			ClientName:        []byte("core"),
			RequestedFeatures: kvCli.requestedFeatures,
		},
		GetErrorMap: &memdx.GetErrorMapRequest{
			Version: 2,
		},
		Auth: &memdx.SaslAuthAutoOptions{
			Username:     opts.Username,
			Password:     opts.Password,
			EnabledMechs: []memdx.AuthMechanism{memdx.ScramSha512AuthMechanism, memdx.ScramSha256AuthMechanism},
		},
		SelectBucket: &memdx.SelectBucketRequest{
			BucketName: opts.SelectedBucket,
		},
		GetClusterConfig: &memdx.GetClusterConfigRequest{},
	})
	if err != nil {
		return nil, err
	}

	log.Printf("bootstrapped: %+v", res.Hello)

	return kvCli, nil
}

func (c *kvClient) Reconfigure(ctx context.Context, opts *KvClientOptions) error {
	return errors.New("kv client does not currently support reconfiguring")
}

func (c *kvClient) HasFeature(feat memdx.HelloFeature) bool {
	return slices.Contains(c.supportedFeatures, feat)
}

func (c *kvClient) Close() error {
	return c.cli.Close()
}

func (c *kvClient) LoadFactor() float64 {
	return (float64)(atomic.LoadUint64(&c.pendingOperations))
}
