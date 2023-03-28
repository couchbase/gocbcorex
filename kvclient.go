package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/couchbase/gocbcorex/memdx"
)

type GetMemdxClientFunc func(opts *memdx.ClientOptions) MemdxDispatcherCloser

type KvClientConfig struct {
	Address                string
	TlsConfig              *tls.Config
	ClientName             string
	Authenticator          Authenticator
	SelectedBucket         string
	DisableDefaultFeatures bool
	DisableErrorMap        bool

	// DisableBootstrap provides a simple way to validate that all bootstrapping
	// is disabled on the client, mainly used for testing.
	DisableBootstrap bool
}

func (o KvClientConfig) Equals(b *KvClientConfig) bool {
	return o.Address == b.Address &&
		o.TlsConfig == b.TlsConfig &&
		o.ClientName == b.ClientName &&
		o.Authenticator == b.Authenticator &&
		o.SelectedBucket == b.SelectedBucket &&
		o.DisableDefaultFeatures == b.DisableDefaultFeatures &&
		o.DisableErrorMap == b.DisableErrorMap &&
		o.DisableBootstrap == b.DisableBootstrap
}

type KvClientOptions struct {
	Logger         *zap.Logger
	NewMemdxClient GetMemdxClientFunc
}

type KvClientOps interface {
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
}

// KvClient implements a synchronous wrapper around a memdx.Client.
type KvClient interface {
	// Reconfigure reconfigures this KvClient to a new state.
	Reconfigure(config *KvClientConfig, cb func(error)) error

	HasFeature(feat memdx.HelloFeature) bool
	Close() error

	LoadFactor() float64

	RemoteAddress() string

	KvClientOps
}

type MemdxDispatcherCloser interface {
	memdx.Dispatcher
	Close() error
}

type kvClient struct {
	logger *zap.Logger

	pendingOperations uint64
	cli               MemdxDispatcherCloser

	lock          sync.Mutex
	currentConfig KvClientConfig

	supportedFeatures []memdx.HelloFeature

	closed uint32
}

var _ KvClient = (*kvClient)(nil)

func NewKvClient(ctx context.Context, config *KvClientConfig, opts *KvClientOptions) (*kvClient, error) {
	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	kvCli := &kvClient{
		currentConfig: *config,
		logger:        logger,
	}

	logger.Debug("id assigned for " + config.Address)

	var requestedFeatures []memdx.HelloFeature
	if !config.DisableDefaultFeatures {
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
	if config.ClientName != "" || len(requestedFeatures) > 0 {
		bootstrapHello = &memdx.HelloRequest{
			ClientName:        []byte(config.ClientName),
			RequestedFeatures: requestedFeatures,
		}
	}

	var bootstrapGetErrorMap *memdx.GetErrorMapRequest
	if !config.DisableErrorMap {
		bootstrapGetErrorMap = &memdx.GetErrorMapRequest{
			Version: 2,
		}
	}

	var bootstrapAuth *memdx.SaslAuthAutoOptions
	if config.Authenticator != nil {
		username, password, err := config.Authenticator.GetCredentials(ServiceTypeMemd, config.Address)
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
	if config.SelectedBucket != "" {
		bootstrapSelectBucket = &memdx.SelectBucketRequest{
			BucketName: config.SelectedBucket,
		}
	}

	shouldBootstrap := bootstrapHello != nil || bootstrapAuth != nil || bootstrapGetErrorMap != nil

	if shouldBootstrap && config.DisableBootstrap {
		return nil, errors.New("bootstrap was disabled but options requiring bootstrap were specified")
	}

	memdxClientOpts := &memdx.ClientOptions{
		OrphanHandler: nil,
		CloseHandler:  nil,
		Logger:        logger,
	}
	if opts.NewMemdxClient == nil {
		conn, err := memdx.DialConn(ctx, config.Address, &memdx.DialConnOptions{TLSConfig: config.TlsConfig})
		if err != nil {
			return nil, err
		}

		kvCli.cli = memdx.NewClient(conn, memdxClientOpts)
	} else {
		kvCli.cli = opts.NewMemdxClient(memdxClientOpts)
	}

	if shouldBootstrap {
		kvCli.logger.Debug("bootstrapping")
		res, err := kvCli.bootstrap(ctx, &memdx.BootstrapOptions{
			Hello:            bootstrapHello,
			GetErrorMap:      bootstrapGetErrorMap,
			Auth:             bootstrapAuth,
			SelectBucket:     bootstrapSelectBucket,
			GetClusterConfig: nil,
		})
		if err != nil {
			kvCli.logger.Debug("bootstrap failed", zap.Error(err))
			if closeErr := kvCli.Close(); closeErr != nil {
				kvCli.logger.Debug("failed to close connection for KvClient", zap.Error(closeErr))
			}
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

func (c *kvClient) Reconfigure(config *KvClientConfig, cb func(error)) error {
	if config == nil {
		return errors.New("must specify a configuration to reconfigure to")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.logger.Debug("reconfiguring")

	if c.currentConfig.Address != config.Address ||
		c.currentConfig.TlsConfig != config.TlsConfig ||
		c.currentConfig.ClientName != config.ClientName ||
		c.currentConfig.Authenticator != config.Authenticator ||
		c.currentConfig.DisableDefaultFeatures != config.DisableDefaultFeatures ||
		c.currentConfig.DisableErrorMap != config.DisableErrorMap ||
		c.currentConfig.DisableBootstrap != config.DisableBootstrap {
		// pretty much everything triggers a reconfigure
		return errors.New("cannot reconfigure due to conflicting options")
	}

	var selectBucketName string
	if config.SelectedBucket != c.currentConfig.SelectedBucket {
		if c.currentConfig.SelectedBucket != "" {
			return errors.New("cannot reconfigure from one selected bucket to another")
		}

		// because we only support going from no selected bucket to a selected
		// bucket, we simply update the state here and nobody will be permitted to
		// reconfigure unless it fails and set its back to no selected bucket.
		c.currentConfig.SelectedBucket = config.SelectedBucket
		selectBucketName = config.SelectedBucket
	}

	if !c.currentConfig.Equals(config) {
		return errors.New("client config after reconfigure did not match new configuration")
	}

	go func() {
		if selectBucketName != "" {
			err := c.SelectBucket(context.Background(), &memdx.SelectBucketRequest{
				BucketName: selectBucketName,
			})
			if err != nil {
				c.lock.Lock()
				c.currentConfig.SelectedBucket = ""
				c.lock.Unlock()

				cb(err)
				return
			}
		}

		cb(nil)
	}()

	return nil
}

func (c *kvClient) HasFeature(feat memdx.HelloFeature) bool {
	return slices.Contains(c.supportedFeatures, feat)
}

func (c *kvClient) Close() error {
	c.logger.Info("closing")
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.logger.Debug("already closed")
		return nil
	}

	return c.cli.Close()
}

func (c *kvClient) LoadFactor() float64 {
	return (float64)(atomic.LoadUint64(&c.pendingOperations))
}

func (c *kvClient) RemoteAddress() string {
	c.lock.Lock()
	addr := c.currentConfig.Address
	c.lock.Unlock()

	return addr
}
