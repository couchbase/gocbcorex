package core

import (
	"context"
	"crypto/tls"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/couchbase/gocbcorex/memdx"
)

type KvClientConfig struct {
	Logger         *zap.Logger
	Address        string
	TlsConfig      *tls.Config
	SelectedBucket string
	Username       string
	Password       string
}

func (o KvClientConfig) Equals(b *KvClientConfig) bool {
	return o.Address == b.Address &&
		o.TlsConfig == b.TlsConfig &&
		o.SelectedBucket == b.SelectedBucket &&
		o.Username == b.Username &&
		o.Password == b.Password
}

type KvClient interface {
	Reconfigure(ctx context.Context, opts *KvClientConfig) error

	GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error)
	GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) ([]byte, error)
	Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error)
	Set(ctx context.Context, req *memdx.SetRequest) (*memdx.SetResponse, error)
	Delete(ctx context.Context, req *memdx.DeleteRequest) (*memdx.DeleteResponse, error)

	Close() error

	LoadFactor() float64
}

type kvClient struct {
	logger *zap.Logger

	pendingOperations uint64
	cli               *memdx.Client

	hostname  string
	tlsConfig *tls.Config
	bucket    string

	username string
	password string

	supportedFeatures []memdx.HelloFeature
}

var _ KvClient = (*kvClient)(nil)

func NewKvClient(ctx context.Context, opts *KvClientConfig) (*kvClient, error) {
	conn, err := memdx.DialConn(ctx, opts.Address, &memdx.DialConnOptions{TLSConfig: opts.TlsConfig})
	if err != nil {
		return nil, err
	}

	cli := memdx.NewClient(conn, &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	})

	kvCli := &kvClient{
		logger:    loggerOrNop(opts.Logger),
		cli:       cli,
		hostname:  opts.Address,
		tlsConfig: opts.TlsConfig,
		username:  opts.Username,
		password:  opts.Password,
		bucket:    opts.SelectedBucket,
	}

	requestedFeatures := []memdx.HelloFeature{
		memdx.HelloFeatureDatatype,
		memdx.HelloFeatureSeqNo,
		memdx.HelloFeatureXattr,
		memdx.HelloFeatureXerror,
		memdx.HelloFeatureSnappy,
		memdx.HelloFeatureJSON,
		memdx.HelloFeatureUnorderedExec,
		memdx.HelloFeatureDurations,
		memdx.HelloFeaturePreserveExpiry,
		memdx.HelloFeatureReplaceBodyWithXattr,
		memdx.HelloFeatureSelectBucket,
		memdx.HelloFeatureCreateAsDeleted,
		memdx.HelloFeatureAltRequests,
		memdx.HelloFeatureCollections,
	}

	bootstrapOpts := &memdx.BootstrapOptions{
		Hello: &memdx.HelloRequest{
			ClientName:        []byte("core"),
			RequestedFeatures: requestedFeatures,
		},
		GetErrorMap: &memdx.GetErrorMapRequest{
			Version: 2,
		},
		Auth: &memdx.SaslAuthAutoOptions{
			Username:     opts.Username,
			Password:     opts.Password,
			EnabledMechs: []memdx.AuthMechanism{memdx.ScramSha512AuthMechanism, memdx.ScramSha256AuthMechanism},
		},
		GetClusterConfig: &memdx.GetClusterConfigRequest{},
	}

	if opts.SelectedBucket != "" {
		bootstrapOpts.SelectBucket = &memdx.SelectBucketRequest{
			BucketName: opts.SelectedBucket,
		}
	}

	res, err := kvCli.bootstrap(ctx, bootstrapOpts)
	if err != nil {
		return nil, err
	}

	kvCli.logger.Debug("successfully bootstrapped new KvClient",
		zap.Any("features", res.Hello.EnabledFeatures))

	kvCli.supportedFeatures = res.Hello.EnabledFeatures

	return kvCli, nil
}

func (c *kvClient) Reconfigure(ctx context.Context, opts *KvClientConfig) error {
	if opts == nil {
		return nil
	}

	if opts.TlsConfig != c.tlsConfig {
		return placeholderError{"cannot reconfigure tls config"}
	}
	if opts.Address != c.hostname {
		return placeholderError{"cannot reconfigure address"}
	}
	if opts.Username != c.username {
		return placeholderError{"cannot reconfigure username"}
	}
	if opts.Password != c.password {
		return placeholderError{"cannot reconfigure password"}
	}
	if opts.SelectedBucket != c.bucket {
		c.bucket = opts.SelectedBucket
		if err := c.SelectBucket(ctx, &memdx.SelectBucketRequest{
			BucketName: c.bucket,
		}); err != nil {
			return err
		}
	}
	return nil
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
