package core

import (
	"crypto/tls"
	"golang.org/x/exp/slices"
	"sync/atomic"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type KvClient interface {
	HasFeature(feat memdx.HelloFeature) bool

	Dispatch(req *memdx.Packet, handler memdx.DispatchCallback) error
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

type kvClientOptions struct {
	Hostname       string
	TlsConfig      *tls.Config
	SelectedBucket string
	Features       []memdx.HelloFeature

	// TODO: this should probably be swapped out for an authenticator of some sort, which can probably be hotswapped
	// during Reconfigure.
	Username string
	Password string
}

func newKvClient(opts kvClientOptions) (*kvClient, error) {
	conn, err := memdx.DialConn(opts.Hostname, &memdx.DialConnOptions{TLSConfig: opts.TlsConfig})
	if err != nil {
		return nil, err
	}
	cli := memdx.NewClient(conn, &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	return &kvClient{
		cli:               cli,
		hostname:          opts.Hostname,
		tlsConfig:         opts.TlsConfig,
		requestedFeatures: opts.Features,
		username:          opts.Username,
		password:          opts.Password,
		bucket:            opts.SelectedBucket,
	}, nil
}

type kvClientBootstrapResult struct {
	ErrorMap      []byte
	ClusterConfig []byte
}

func (c *kvClient) Bootstrap() (*kvClientBootstrapResult, error) {
	errWait := make(chan error, 1)
	resultWait := make(chan *kvClientBootstrapResult, 1)
	memdx.OpBootstrap{
		Encoder: memdx.OpsCore{},
	}.Execute(c, &memdx.BootstrapOptions{
		Hello: &memdx.HelloRequest{
			ClientName:        []byte("core"),
			RequestedFeatures: c.requestedFeatures,
		},
		GetErrorMap: &memdx.GetErrorMapRequest{
			Version: 2,
		},
		Auth: &memdx.SaslAuthAutoOptions{
			Username:     c.username,
			Password:     c.password,
			EnabledMechs: []memdx.AuthMechanism{memdx.ScramSha512AuthMechanism, memdx.ScramSha256AuthMechanism},
		},
		SelectBucket: &memdx.SelectBucketRequest{
			BucketName: c.bucket,
		},
		GetClusterConfig: &memdx.GetClusterConfigRequest{},
	}, func(res *memdx.BootstrapResult, err error) {
		if err != nil {
			errWait <- err
			return
		}
		c.supportedFeatures = res.Hello.EnabledFeatures

		resultWait <- &kvClientBootstrapResult{
			ErrorMap:      res.ErrorMap,
			ClusterConfig: res.ClusterConfig,
		}
	})

	select {
	case err := <-errWait:
		return nil, err
	case res := <-resultWait:
		return res, nil
	}
	// HELLO
	// GET_ERROR_MAP
	// AUTH (SASL_LIST_MECHS, SASL_AUTH, SASL_CONTINUE)
	// SELECT_BUCKET
	// GET_CLUSTER_CONFIG
	// OPERATIONS
}

func (c *kvClient) HasFeature(feat memdx.HelloFeature) bool {
	return slices.Contains(c.supportedFeatures, feat)
}

func (c *kvClient) Dispatch(req *memdx.Packet, handler memdx.DispatchCallback) error {
	return c.cli.Dispatch(req, handler)
}

func (c *kvClient) Close() error {
	return c.cli.Close()
}

func (c *kvClient) LoadFactor() float64 {
	return (float64)(atomic.LoadUint64(&c.pendingOperations))
}
