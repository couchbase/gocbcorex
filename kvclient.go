package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/gocbcorex/memdx"
)

type GetMemdxClientFunc func(opts *memdx.ClientOptions) MemdxClient

type DcpClientEventsHandlers struct {
	SnapshotMarker     func(req *memdx.DcpSnapshotMarkerEvent) error
	Mutation           func(req *memdx.DcpMutationEvent) error
	Deletion           func(req *memdx.DcpDeletionEvent) error
	Expiration         func(req *memdx.DcpExpirationEvent) error
	CollectionCreation func(req *memdx.DcpCollectionCreationEvent) error
	CollectionDeletion func(req *memdx.DcpCollectionDeletionEvent) error
	CollectionFlush    func(req *memdx.DcpCollectionFlushEvent) error
	ScopeCreation      func(req *memdx.DcpScopeCreationEvent) error
	ScopeDeletion      func(req *memdx.DcpScopeDeletionEvent) error
	CollectionChanged  func(req *memdx.DcpCollectionModificationEvent) error
	StreamEnd          func(req *memdx.DcpStreamEndEvent) error
	OSOSnapshot        func(req *memdx.DcpOSOSnapshotEvent) error
	SeqNoAdvanced      func(req *memdx.DcpSeqNoAdvancedEvent) error
}

type KvClientConfig struct {
	Address        string
	TlsConfig      *tls.Config
	ClientName     string
	Authenticator  Authenticator
	SelectedBucket string
}

func (o KvClientConfig) Equals(b *KvClientConfig) bool {
	return o.Address == b.Address &&
		o.TlsConfig == b.TlsConfig &&
		o.ClientName == b.ClientName &&
		o.Authenticator == b.Authenticator &&
		o.SelectedBucket == b.SelectedBucket
}

type KvClientDcpConfig struct {
	Handlers DcpClientEventsHandlers

	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	NoopInterval          time.Duration
	Priority              string
	ForceValueCompression bool
	EnableExpiryEvents    bool
	EnableStreamIds       bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool
}

func (o KvClientDcpConfig) Equals(b *KvClientDcpConfig) bool {
	return o.Handlers == b.Handlers &&
		o.ConnectionName == b.ConnectionName &&
		o.ConsumerName == b.ConsumerName &&
		o.ConnectionFlags == b.ConnectionFlags &&
		o.NoopInterval == b.NoopInterval &&
		o.Priority == b.Priority &&
		o.ForceValueCompression == b.ForceValueCompression &&
		o.EnableExpiryEvents == b.EnableExpiryEvents &&
		o.EnableStreamIds == b.EnableStreamIds &&
		o.EnableOso == b.EnableOso &&
		o.EnableSeqNoAdvance == b.EnableSeqNoAdvance &&
		o.BackfillOrder == b.BackfillOrder &&
		o.EnableChangeStreams == b.EnableChangeStreams
}

type KvClientOptions struct {
	Logger         *zap.Logger
	NewMemdxClient GetMemdxClientFunc
	CloseHandler   func(KvClient, error)

	DisableDefaultFeatures bool
	DisableErrorMap        bool

	// DisableBootstrap provides a simple way to validate that all bootstrapping
	// is disabled on the client, mainly used for testing.
	DisableBootstrap bool

	DcpOptions *KvClientDcpOptions
}

type KvClientOps interface {
	GetCollectionID(ctx context.Context, req *memdx.GetCollectionIDRequest) (*memdx.GetCollectionIDResponse, error)
	GetClusterConfig(ctx context.Context, req *memdx.GetClusterConfigRequest) (*memdx.GetClusterConfigResponse, error)
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
	AddWithMeta(ctx context.Context, req *memdx.AddWithMetaRequest) (*memdx.AddWithMetaResponse, error)
	SetWithMeta(ctx context.Context, req *memdx.SetWithMetaRequest) (*memdx.SetWithMetaResponse, error)
	DeleteWithMeta(ctx context.Context, req *memdx.DeleteWithMetaRequest) (*memdx.DeleteWithMetaResponse, error)
	LookupIn(ctx context.Context, req *memdx.LookupInRequest) (*memdx.LookupInResponse, error)
	MutateIn(ctx context.Context, req *memdx.MutateInRequest) (*memdx.MutateInResponse, error)
	RangeScanCreate(ctx context.Context, req *memdx.RangeScanCreateRequest) (*memdx.RangeScanCreateResponse, error)
	RangeScanContinue(ctx context.Context, req *memdx.RangeScanContinueRequest,
		dataCb func(*memdx.RangeScanDataResponse) error) (*memdx.RangeScanActionResponse, error)
	RangeScanCancel(ctx context.Context, req *memdx.RangeScanCancelRequest) (*memdx.RangeScanCancelResponse, error)
	Stats(ctx context.Context, req *memdx.StatsRequest, dataCb func(*memdx.StatsDataResponse) error) (*memdx.StatsActionResponse, error)
}

// KvClient implements a synchronous wrapper around a memdx.Client.
type KvClient interface {
	// Reconfigure reconfigures this KvClient to a new state.
	Reconfigure(config *KvClientConfig, cb func(error)) error

	HasFeature(feat memdx.HelloFeature) bool
	Close() error

	LoadFactor() float64

	RemoteHostname() string
	RemoteAddr() net.Addr
	LocalAddr() net.Addr

	KvClientOps
	memdx.Dispatcher
}

type kvClient struct {
	logger         *zap.Logger
	remoteHostname string

	pendingOperations uint64
	cli               MemdxClient
	telemetry         *kvClientTelem

	lock          sync.Mutex
	currentConfig KvClientConfig

	supportedFeatures               []memdx.HelloFeature
	dcpNoopEnabled                  bool
	dcpStreamEndOnCloseEnabled      bool
	dcpPriority                     string
	dcpForceValueCompressionEnabled bool
	dcpExpiryEventsEnabled          bool
	dcpStreamIdsEnabled             bool
	dcpOsoEnabled                   bool
	dcpSeqNoAdvancedEnabled         bool
	dcpBackfillOrder                string
	dcpChangeStreamsEnabled         bool

	// selectedBucket atomically stores the currently selected bucket,
	// so that we can use it in our errors.  Note that it is set before
	// we send the operation to select the bucket, since things happen
	// asynchronously and we do not support changing selected buckets.
	selectedBucket atomic.Pointer[string]

	closed       uint32
	closeHandler func(KvClient, error)
}

var _ KvClient = (*kvClient)(nil)

func NewKvClient(ctx context.Context, config *KvClientConfig, opts *KvClientOptions) (*kvClient, error) {
	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	kvCli := &kvClient{
		currentConfig:  *config,
		remoteHostname: hostnameFromAddrStr(config.Address),
		logger:         logger,
		closeHandler:   opts.CloseHandler,
	}

	logger.Debug("id assigned for " + config.Address)

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
	if config.ClientName != "" || len(requestedFeatures) > 0 {
		bootstrapHello = &memdx.HelloRequest{
			ClientName:        []byte(config.ClientName),
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

	if opts.DcpOptions != nil {
		dcpOptions := opts.DcpOptions

		if dcpOptions.ConnectionName == "" {
			return nil, errors.New("connection name must be specified")
		}
		if config.SelectedBucket == "" {
			return nil, errors.New("bucket name must be specified")
		}
		if (dcpOptions.ConnectionFlags & memdx.DcpConnectionFlagsProducer) == 0 {
			return nil, errors.New("dcp client only supports producer mode")
		}
	}

	shouldBootstrap := bootstrapHello != nil || bootstrapAuth != nil || bootstrapGetErrorMap != nil || opts.DcpOptions != nil

	if shouldBootstrap && opts.DisableBootstrap {
		return nil, errors.New("bootstrap was disabled but options requiring bootstrap were specified")
	}

	memdxClientOpts := &memdx.ClientOptions{
		UnsolicitedHandler: kvCli.handleUnsolicitedPacket,
		OrphanHandler:      kvCli.handleOrphanResponse,
		ReadErrorHandler:   kvCli.handleConnectionReadError,
		Logger:             logger,
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

	kvCli.telemetry = newKvClientTelem(kvCli.cli.LocalAddr(), kvCli.cli.RemoteAddr())

	if shouldBootstrap {
		if bootstrapSelectBucket != nil {
			kvCli.selectedBucket.Store(ptr.To(bootstrapSelectBucket.BucketName))
		}

		closeConnection := func() {
			if closeErr := kvCli.Close(); closeErr != nil {
				kvCli.logger.Debug("failed to close connection for DcpClient", zap.Error(closeErr))
			}
		}

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
			closeConnection()

			return nil, contextualError{
				Message: "failed to bootstrap",
				Cause:   err,
			}
		}

		if res.Hello != nil {
			kvCli.supportedFeatures = res.Hello.EnabledFeatures
		}

		kvCli.logger.Debug("successfully bootstrapped new KvClient",
			zap.Stringers("features", kvCli.supportedFeatures))

		if opts.DcpOptions != nil {
			dcpOpts := opts.DcpOptions

			_, err = kvCli.dcpOpenConnection(ctx, &memdx.DcpOpenConnectionRequest{
				ConnectionName: dcpOpts.ConnectionName,
				ConsumerName:   dcpOpts.ConsumerName,
				Flags:          dcpOpts.ConnectionFlags,
			})
			if err != nil {
				closeConnection()
				return nil, contextualError{
					Message: "dcp openconnection failed",
					Cause:   err,
				}
			}

			_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
				Key:   "send_stream_end_on_client_close_stream",
				Value: "true",
			})
			if err != nil {
				kvCli.logger.Debug("failed to enable stream-end-on-close feature", zap.Error(err))
			} else {
				kvCli.dcpStreamEndOnCloseEnabled = true
			}

			if dcpOpts.NoopInterval > 0 {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "set_noop_interval",
					Value: fmt.Sprintf("%d", dcpOpts.NoopInterval/time.Second),
				})
				if err == nil {
					_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
						Key:   "enable_noop",
						Value: "true",
					})
				}
				if err != nil {
					kvCli.logger.Debug("noop requested, but could not be enabled", zap.Error(err))
				} else {
					kvCli.dcpNoopEnabled = true
				}
			}

			if dcpOpts.Priority != "" {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "set_priority",
					Value: dcpOpts.Priority,
				})
				if err != nil {
					kvCli.logger.Debug("failed to set dcp priority", zap.Error(err))
				} else {
					kvCli.dcpPriority = dcpOpts.Priority
				}
			}

			if dcpOpts.ForceValueCompression {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "force_value_compression",
					Value: "true",
				})
				if err != nil {
					kvCli.logger.Debug("failed to enable forced value compression", zap.Error(err))
				} else {
					kvCli.dcpForceValueCompressionEnabled = true
				}
			}

			if dcpOpts.EnableExpiryEvents {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "enable_expiry_opcode",
					Value: "true",
				})
				if err != nil {
					kvCli.logger.Debug("failed to enable expiry events feature", zap.Error(err))
				} else {
					kvCli.dcpExpiryEventsEnabled = true
				}
			}

			if dcpOpts.EnableStreamIds {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "enable_stream_id",
					Value: "true",
				})
				if err != nil {
					kvCli.logger.Debug("failed to enable stream ids", zap.Error(err))
				} else {
					kvCli.dcpStreamIdsEnabled = true
				}
			}

			if dcpOpts.EnableOso {
				if dcpOpts.EnableSeqNoAdvance {
					_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
						Key:   "enable_out_of_order_snapshots",
						Value: "true_with_seqno_advanced",
					})
					if err != nil {
						kvCli.logger.Debug("failed to enable oso with seqno advanced", zap.Error(err))
					} else {
						kvCli.dcpOsoEnabled = true
						kvCli.dcpSeqNoAdvancedEnabled = true
					}
				}

				if !kvCli.dcpOsoEnabled {
					_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
						Key:   "enable_out_of_order_snapshots",
						Value: "true",
					})
					if err != nil {
						kvCli.logger.Debug("failed to enable oso", zap.Error(err))
					} else {
						kvCli.dcpOsoEnabled = true
					}
				}
			}

			if dcpOpts.BackfillOrder != "" {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "backfill_order",
					Value: dcpOpts.BackfillOrder,
				})
				if err != nil {
					kvCli.logger.Debug("failed to set backfill order", zap.Error(err))
				} else {
					kvCli.dcpBackfillOrder = dcpOpts.BackfillOrder
				}
			}

			if dcpOpts.EnableChangeStreams {
				_, err = kvCli.dcpControl(ctx, &memdx.DcpControlRequest{
					Key:   "change_streams",
					Value: "true",
				})
				if err != nil {
					kvCli.logger.Debug("failed to enable change streams", zap.Error(err))
				} else {
					kvCli.dcpChangeStreamsEnabled = true
				}
			}

			kvCli.logger.Debug("successfully configured new DcpClient")
		}
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
		c.currentConfig.Authenticator != config.Authenticator {
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
			c.selectedBucket.Store(ptr.To(selectBucketName))

			_, err := c.SelectBucket(context.Background(), &memdx.SelectBucketRequest{
				BucketName: selectBucketName,
			})
			if err != nil {
				c.lock.Lock()
				c.currentConfig.SelectedBucket = ""
				c.selectedBucket.Store(ptr.To(""))
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

func (c *kvClient) RemoteHostname() string {
	return c.remoteHostname
}

func (c *kvClient) RemoteAddr() net.Addr {
	return c.cli.RemoteAddr()
}

func (c *kvClient) LocalAddr() net.Addr {
	return c.cli.LocalAddr()
}

func (c *kvClient) WritePacket(pak *memdx.Packet) error {
	return c.cli.WritePacket(pak)
}

func (c *kvClient) Dispatch(pak *memdx.Packet, cb memdx.DispatchCallback) (memdx.PendingOp, error) {
	return c.cli.Dispatch(pak, cb)
}

func (c *kvClient) SelectedBucket() string {
	bucketNamePtr := c.selectedBucket.Load()
	if bucketNamePtr != nil {
		return *bucketNamePtr
	}
	return ""
}

func (c *kvClient) Telemetry() MemdClientTelem {
	return c.telemetry
}

func (c *kvClient) handleUnsolicitedPacket(pak *memdx.Packet) {
	c.logger.Info("unexpected unsolicited packet",
		zap.String("opaque", strconv.Itoa(int(pak.Opaque))),
		zap.String("opcode", pak.OpCode.String()))
}

func (c *kvClient) handleOrphanResponse(pak *memdx.Packet) {
	c.logger.Info(
		"orphaned response encountered",
		zap.String("opaque", strconv.Itoa(int(pak.Opaque))),
		zap.String("opcode", pak.OpCode.String()),
	)
}

func (c *kvClient) handleConnectionReadError(err error) {
	// Just mark ourselves as closed. The connection is already
	// closed so there's no actual work to do, and we might already actually be closed.
	atomic.StoreUint32(&c.closed, 1)

	if c.closeHandler != nil {
		c.closeHandler(c, err)
	}
}
