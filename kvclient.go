package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/couchbase/gocbcorex/memdx"
)

type GetMemdxClientFunc func(opts *memdx.ClientOptions) MemdxDispatcherCloser

type KvClientConfig struct {
	Logger                 *zap.Logger
	Address                string
	TlsConfig              *tls.Config
	Authenticator          Authenticator
	ClientName             string
	SelectedBucket         string
	DisableDefaultFeatures bool
	DisableErrorMap        bool

	// DisableBootstrap provides a simple way to validate that all bootstrapping
	// is disabled on the client, mainly used for testing.
	DisableBootstrap bool

	NewMemdxClient GetMemdxClientFunc
}

func (o KvClientConfig) Equals(b *KvClientConfig) bool {
	return o.Address == b.Address &&
		o.TlsConfig == b.TlsConfig &&
		o.SelectedBucket == b.SelectedBucket &&
		o.Authenticator == b.Authenticator
}

type KvClient interface {
	Reconfigure(ctx context.Context, opts *KvClientConfig) error

	GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error)
	GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) ([]byte, error)
	Get(ctx context.Context, req *memdx.GetRequest) (*memdx.GetResponse, error)
	Set(ctx context.Context, req *memdx.SetRequest) (*memdx.SetResponse, error)
	Delete(ctx context.Context, req *memdx.DeleteRequest) (*memdx.DeleteResponse, error)
	GetAndLock(ctx context.Context, req *memdx.GetAndLockRequest) (*memdx.GetAndLockResponse, error)
	GetAndTouch(ctx context.Context, req *memdx.GetAndTouchRequest) (*memdx.GetAndTouchResponse, error)
	GetReplica(ctx context.Context, req *memdx.GetReplicaRequest) (*memdx.GetReplicaResponse, error)
	GetRandom(ctx context.Context, req *memdx.GetRandomRequest) (*memdx.GetRandomResponse, error)
	Unlock(ctx context.Context, req *memdx.UnlockRequest) (*memdx.UnlockResponse, error)
	Touch(ctx context.Context, req *memdx.TouchRequest) (*memdx.TouchResponse, error)
	Add(ctx context.Context, req *memdx.AddRequest) (*memdx.AddResponse, error)
	Replace(ctx context.Context, req *memdx.ReplaceRequest) (*memdx.ReplaceResponse, error)
	Append(ctx context.Context, req *memdx.AppendRequest) (*memdx.AppendResponse, error)
	Prepend(ctx context.Context, req *memdx.PrependRequest) (*memdx.PrependResponse, error)
	Increment(ctx context.Context, req *memdx.IncrementRequest) (*memdx.IncrementResponse, error)
	Decrement(ctx context.Context, req *memdx.DecrementRequest) (*memdx.DecrementResponse, error)
	GetMeta(ctx context.Context, req *memdx.GetMetaRequest) (*memdx.GetMetaResponse, error)
	SetMeta(ctx context.Context, req *memdx.SetMetaRequest) (*memdx.SetMetaResponse, error)
	DeleteMeta(ctx context.Context, req *memdx.DeleteMetaRequest) (*memdx.DeleteMetaResponse, error)
	LookupIn(ctx context.Context, req *memdx.LookupInRequest) (*memdx.LookupInResponse, error)
	MutateIn(ctx context.Context, req *memdx.MutateInRequest) (*memdx.MutateInResponse, error)

	HasFeature(feat memdx.HelloFeature) bool

	Close() error

	LoadFactor() float64
}

type MemdxDispatcherCloser interface {
	memdx.Dispatcher
	Close() error
}

type kvClient struct {
	logger *zap.Logger

	pendingOperations uint64
	cli               MemdxDispatcherCloser

	hostname      string
	tlsConfig     *tls.Config
	authenticator Authenticator
	bucket        string

	supportedFeatures []memdx.HelloFeature
}

var _ KvClient = (*kvClient)(nil)

func NewKvClient(ctx context.Context, opts *KvClientConfig) (*kvClient, error) {
	kvCli := &kvClient{
		logger:        loggerOrNop(opts.Logger),
		hostname:      opts.Address,
		tlsConfig:     opts.TlsConfig,
		authenticator: opts.Authenticator,
		bucket:        opts.SelectedBucket,
	}

	memdxClientOpts := &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
	}
	if opts.NewMemdxClient == nil {
		conn, err := memdx.DialConn(ctx, opts.Address, &memdx.DialConnOptions{TLSConfig: opts.TlsConfig})
		if err != nil {
			return nil, err
		}

		kvCli.cli = memdx.NewClient(conn, memdxClientOpts)
	} else {
		kvCli.cli = opts.NewMemdxClient(memdxClientOpts)
	}

	var requestedFeatures []memdx.HelloFeature
	if !opts.DisableDefaultFeatures {
		requestedFeatures = []memdx.HelloFeature{
			memdx.HelloFeatureDatatype,
			memdx.HelloFeatureSeqNo,
			memdx.HelloFeatureXattr,
			memdx.HelloFeatureXerror,
			memdx.HelloFeatureSnappy,
			memdx.HelloFeatureJSON,
			memdx.HelloFeatureUnorderedExec,
			memdx.HelloFeatureDurations,
			memdx.HelloFeaturePreserveExpiry,
			memdx.HelloFeatureSyncReplication,
			memdx.HelloFeatureReplaceBodyWithXattr,
			memdx.HelloFeatureSelectBucket,
			memdx.HelloFeatureCreateAsDeleted,
			memdx.HelloFeatureAltRequests,
			memdx.HelloFeatureCollections,
		}
	}

	var bootstrapHello *memdx.HelloRequest
	if opts.ClientName != "" || len(requestedFeatures) > 0 {
		bootstrapHello = &memdx.HelloRequest{
			ClientName:        []byte(opts.ClientName),
			RequestedFeatures: requestedFeatures,
		}
	}

	var bootstrapGetErrorMap *memdx.GetErrorMapRequest
	if !opts.DisableErrorMap {
		bootstrapGetErrorMap = &memdx.GetErrorMapRequest{
			Version: 2,
		}
	}

	var bootstrapAuth *memdx.SaslAuthAutoOptions
	if opts.Authenticator != nil {
		username, password, err := opts.Authenticator.GetCredentials(MemdService, opts.Address)
		if err != nil {
			return nil, err
		}

		bootstrapAuth = &memdx.SaslAuthAutoOptions{
			Username: username,
			Password: password,
			EnabledMechs: []memdx.AuthMechanism{
				memdx.ScramSha512AuthMechanism,
				memdx.ScramSha256AuthMechanism},
		}
	}

	var bootstrapSelectBucket *memdx.SelectBucketRequest
	if opts.SelectedBucket != "" {
		bootstrapSelectBucket = &memdx.SelectBucketRequest{
			BucketName: opts.SelectedBucket,
		}
	}

	if bootstrapHello != nil || bootstrapAuth != nil || bootstrapGetErrorMap != nil {
		if opts.DisableBootstrap {
			return nil, errors.New("bootstrap was disabled but options requiring bootstrap were specified")
		}

		res, err := kvCli.bootstrap(ctx, &memdx.BootstrapOptions{
			Hello:            bootstrapHello,
			GetErrorMap:      bootstrapGetErrorMap,
			Auth:             bootstrapAuth,
			SelectBucket:     bootstrapSelectBucket,
			GetClusterConfig: nil,
		})
		if err != nil {
			return nil, err
		}

		kvCli.logger.Debug("successfully bootstrapped new KvClient",
			zap.Any("features", res.Hello.EnabledFeatures))

		kvCli.supportedFeatures = res.Hello.EnabledFeatures
	} else {
		kvCli.logger.Debug("skipped bootstrapping new KvClient")
	}

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
	if opts.Authenticator != c.authenticator {
		return placeholderError{"cannot reconfigure authenticator"}
	}

	if opts.SelectedBucket != "" {
		if c.bucket != "" {
			return placeholderError{"cannot perform select bucket on an already bucket bound kvclient"}
		}

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
