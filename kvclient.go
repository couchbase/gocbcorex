package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/gocbcorex/memdx"
)

type KvDispatchNetError struct {
	Cause error
}

func (e KvDispatchNetError) Error() string {
	if e.Cause != nil {
		return "dispatch error: " + e.Cause.Error()
	}
	return "dispatch error"
}

func (e KvDispatchNetError) Unwrap() error {
	return e.Cause
}

type DialMemdxClientFunc = func(
	ctx context.Context,
	address string,
	dialOpts *memdx.DialConnOptions,
	clientOpts *memdx.ClientOptions,
) (MemdxClient, error)

type KvClientBootstrapOptions struct {
	ClientName string

	DisableDefaultFeatures bool
	DisableErrorMap        bool

	// DisableBootstrap provides a simple way to validate that all bootstrapping
	// is disabled on the client, mainly used for testing.
	DisableBootstrap bool
}

func (v KvClientBootstrapOptions) Equals(o KvClientBootstrapOptions) bool {
	return v.ClientName == o.ClientName &&
		v.DisableDefaultFeatures == o.DisableDefaultFeatures &&
		v.DisableErrorMap == o.DisableErrorMap &&
		v.DisableBootstrap == o.DisableBootstrap
}

type KvClientOptions struct {
	Logger *zap.Logger

	Address        string
	TlsConfig      *tls.Config
	Auth           *memdx.SaslAuthAutoOptions
	SelectedBucket string
	BootstrapOpts  KvClientBootstrapOptions
	DcpOpts        *KvClientDcpOptions

	DialMemdxClient DialMemdxClientFunc
	CloseHandler    func(KvClient, error)
	DcpHandlers     KvClientDcpEventsHandlers
}

// KvClient implements a synchronous wrapper around a memdx.Client.
type KvClient interface {
	SelectBucket(ctx context.Context, bucketName string) error

	HasFeature(feat memdx.HelloFeature) bool
	Close() error

	RemoteHostname() string
	RemoteAddr() net.Addr
	LocalAddr() net.Addr

	KvClientOps
	memdx.Dispatcher
}

type kvClient struct {
	logger         *zap.Logger
	remoteHostname string

	cli       MemdxClient
	telemetry *kvClientTelem

	supportedFeatures []memdx.HelloFeature
	dcpState          *KvClientDcpState

	// selectedBucket atomically stores the currently selected bucket,
	// so that we can use it in our errors.  Note that it is set before
	// we send the operation to select the bucket, since things happen
	// asynchronously and we do not support changing selected buckets.
	selectedBucket atomic.Pointer[string]

	closed       atomic.Bool
	closeHandler func(KvClient, error)
	dcpHandlers  KvClientDcpEventsHandlers
}

var _ KvClient = (*kvClient)(nil)

func NewKvClient(ctx context.Context, opts *KvClientOptions) (KvClient, error) {
	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	if opts.DcpOpts != nil {
		dcpOpts := opts.DcpOpts

		if dcpOpts.ConnectionName == "" {
			return nil, errors.New("dcp connection name must be specified")
		}
		if opts.SelectedBucket == "" {
			return nil, errors.New("bucket name must be specified when using dcp")
		}
		if (dcpOpts.ConnectionFlags & memdx.DcpConnectionFlagsProducer) == 0 {
			return nil, errors.New("dcp client only supports producer mode")
		}
	}

	dialMemdxClient := opts.DialMemdxClient
	if dialMemdxClient == nil {
		dialMemdxClient = DialMemdxClient
	}

	kvCli := &kvClient{
		remoteHostname: hostnameFromAddrStr(opts.Address),
		logger:         logger,
		closeHandler:   opts.CloseHandler,
		dcpHandlers:    opts.DcpHandlers,
	}

	logger.Debug("id assigned for " + opts.Address)

	var requestedFeatures []memdx.HelloFeature
	if !opts.BootstrapOpts.DisableDefaultFeatures {
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
	if opts.BootstrapOpts.ClientName != "" || len(requestedFeatures) > 0 {
		bootstrapHello = &memdx.HelloRequest{
			ClientName:        []byte(opts.BootstrapOpts.ClientName),
			RequestedFeatures: requestedFeatures,
		}
	}

	var bootstrapGetErrorMap *memdx.GetErrorMapRequest
	if !opts.BootstrapOpts.DisableErrorMap {
		bootstrapGetErrorMap = &memdx.GetErrorMapRequest{
			Version: 2,
		}
	}

	var bootstrapAuth *memdx.SaslAuthAutoOptions
	if opts.Auth != nil {
		bootstrapAuth = opts.Auth
	}

	var bootstrapSelectBucket *memdx.SelectBucketRequest
	if opts.SelectedBucket != "" {
		bootstrapSelectBucket = &memdx.SelectBucketRequest{
			BucketName: opts.SelectedBucket,
		}
	}

	shouldBootstrap := bootstrapHello != nil || bootstrapAuth != nil || bootstrapGetErrorMap != nil || opts.DcpOpts != nil

	if shouldBootstrap && opts.BootstrapOpts.DisableBootstrap {
		return nil, errors.New("bootstrap was disabled but options requiring bootstrap were specified")
	}

	client, err := dialMemdxClient(
		ctx,
		opts.Address,
		&memdx.DialConnOptions{
			TLSConfig: opts.TlsConfig,
		},
		&memdx.ClientOptions{
			UnsolicitedHandler: kvCli.handleUnsolicitedPacket,
			OrphanHandler:      kvCli.handleOrphanResponse,
			ReadErrorHandler:   kvCli.handleConnectionReadError,
			Logger:             logger,
		})
	if err != nil {
		return nil, err
	}
	kvCli.cli = client

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

		if opts.DcpOpts != nil {
			kvCli.logger.Debug("configuring dcp", zap.Any("dcpOpts", opts.DcpOpts))

			dcpState, err := kvCli.bootstrapDcp(ctx, opts.DcpOpts)
			if err != nil {
				kvCli.logger.Debug("dcp config failed", zap.Error(err))
				closeConnection()

				return nil, contextualError{
					Message: "failed to configure dcp",
					Cause:   err,
				}
			}

			kvCli.dcpState = dcpState

			kvCli.logger.Debug("successfully configured dcp")
		}
	} else {
		kvCli.logger.Debug("skipped bootstrapping new KvClient")
	}

	return kvCli, nil
}

func (c *kvClient) SelectBucket(ctx context.Context, bucketName string) error {
	if bucketName == "" {
		return errors.New("bucket name cannot be empty")
	}

	c.logger.Debug("selecting bucket", zap.String("bucketName", bucketName))

	selectedBucket := c.selectedBucket.Load()
	if selectedBucket != nil {
		return errors.New("cannot reconfigure from one selected bucket to another")
	}

	c.selectedBucket.Store(ptr.To(bucketName))

	_, err := c.selectBucket(ctx, &memdx.SelectBucketRequest{
		BucketName: bucketName,
	})
	if err != nil {
		c.selectedBucket.Store(nil)
		return err
	}

	return nil
}

func (c *kvClient) HasFeature(feat memdx.HelloFeature) bool {
	return slices.Contains(c.supportedFeatures, feat)
}

func (c *kvClient) markClosed(err error) bool {
	if !c.closed.CompareAndSwap(false, true) {
		return false
	}

	closeErr := c.cli.Close()
	if closeErr != nil {
		if !errors.Is(closeErr, net.ErrClosed) {
			c.logger.Debug("failed to close connection for KvClient", zap.Error(closeErr))
		}
	}

	if c.closeHandler != nil {
		c.closeHandler(c, err)
	}

	return true
}

func (c *kvClient) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return net.ErrClosed
	}

	err := c.cli.Close()
	if err != nil {
		return err
	}

	if c.closeHandler != nil {
		c.closeHandler(c, err)
	}

	return nil
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
	op, err := c.cli.Dispatch(pak, cb)
	if err != nil {
		var netErr *net.OpError
		if errors.As(err, &netErr) {
			c.logger.Debug("dispatch failed", zap.Error(err))
			c.markClosed(err)
			return nil, &KvDispatchNetError{Cause: err}
		}

		return nil, err
	}

	return op, nil
}

func (c *kvClient) SelectedBucket() string {
	bucketNamePtr := c.selectedBucket.Load()
	if bucketNamePtr != nil {
		return *bucketNamePtr
	}
	return ""
}

func (c *kvClient) DcpState() KvClientDcpState {
	// DCP state is only set when DCP is initially configured, and is immutable
	// after that, so we can safely return a copy of it without locking.

	if c.dcpState == nil {
		return KvClientDcpState{}
	}

	return *c.dcpState
}

func (c *kvClient) Telemetry() MemdClientTelem {
	return c.telemetry
}

func (c *kvClient) handleUnsolicitedPacket(pak *memdx.Packet) {
	err := memdx.UnsolicitedOpsParser{
		CollectionsEnabled: c.HasFeature(memdx.HelloFeatureCollections),
	}.Handle(c, pak, &memdx.UnsolicitedOpsHandlers{
		DcpSnapshotMarker:     c.dcpHandlers.SnapshotMarker,
		DcpMutation:           c.dcpHandlers.Mutation,
		DcpDeletion:           c.dcpHandlers.Deletion,
		DcpExpiration:         c.dcpHandlers.Expiration,
		DcpCollectionCreation: c.dcpHandlers.CollectionCreation,
		DcpCollectionDeletion: c.dcpHandlers.CollectionDeletion,
		DcpCollectionFlush:    c.dcpHandlers.CollectionFlush,
		DcpScopeCreation:      c.dcpHandlers.ScopeCreation,
		DcpScopeDeletion:      c.dcpHandlers.ScopeDeletion,
		DcpCollectionChanged:  c.dcpHandlers.CollectionChanged,
		DcpStreamEnd:          c.dcpHandlers.StreamEnd,
		DcpOSOSnapshot:        c.dcpHandlers.OSOSnapshot,
		DcpSeqNoAdvanced:      c.dcpHandlers.SeqNoAdvanced,
		DcpNoOp: func(evt *memdx.DcpNoOpEvent) (*memdx.DcpNoOpEventResponse, error) {
			return &memdx.DcpNoOpEventResponse{}, nil
		},
	})
	if err != nil {
		c.logger.Info("error handling unsolicited packet",
			zap.Error(err))
	}
}

func (c *kvClient) handleOrphanResponse(pak *memdx.Packet) {
	c.logger.Info(
		"orphaned response encountered",
		zap.String("opaque", strconv.Itoa(int(pak.Opaque))),
		zap.String("opcode", pak.OpCode.String()),
	)
}

func (c *kvClient) handleConnectionReadError(err error) {
	c.logger.Debug("received connection read error", zap.Error(err))
	c.markClosed(err)
}
