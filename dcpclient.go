package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type DcpClientEventsHandlers struct {
	DcpSnapshotMarker     func(req *memdx.DcpSnapshotMarkerEvent) error
	DcpMutation           func(req *memdx.DcpMutationEvent) error
	DcpDeletion           func(req *memdx.DcpDeletionEvent) error
	DcpExpiration         func(req *memdx.DcpExpirationEvent) error
	DcpCollectionCreation func(req *memdx.DcpCollectionCreationEvent) error
	DcpCollectionDeletion func(req *memdx.DcpCollectionDeletionEvent) error
	DcpCollectionFlush    func(req *memdx.DcpCollectionFlushEvent) error
	DcpScopeCreation      func(req *memdx.DcpScopeCreationEvent) error
	DcpScopeDeletion      func(req *memdx.DcpScopeDeletionEvent) error
	DcpCollectionChanged  func(req *memdx.DcpCollectionModificationEvent) error
	DcpStreamEnd          func(req *memdx.DcpStreamEndEvent) error
	DcpOSOSnapshot        func(req *memdx.DcpOSOSnapshotEvent) error
	DcpSeqNoAdvanced      func(req *memdx.DcpSeqoNoAdvancedEvent) error
}

type DcpClientOptions struct {
	Address        string
	TlsConfig      *tls.Config
	ClientName     string
	Authenticator  Authenticator
	SelectedBucket string

	Handlers DcpClientEventsHandlers

	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	EnableStreamIds       bool
	NoopInterval          time.Duration
	BufferSize            int
	Priority              string
	ForceValueCompression bool
	EnableCursorDropping  bool
	EnableExpiryEvents    bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool

	Logger         *zap.Logger
	NewMemdxClient GetMemdxClientFunc
	CloseHandler   func(KvClient, error)
}

type DcpClient struct {
	logger         *zap.Logger
	selectedBucket string
	handlers       DcpClientEventsHandlers

	cli                     MemdxClient
	supportedFeatures       []memdx.HelloFeature
	noopEnabled             bool
	streamIdsEnabled        bool
	streamEndOnCloseEnabled bool

	closed uint32
}

var _ MemdClient = (*DcpClient)(nil)

func NewDcpClient(ctx context.Context, opts *DcpClientOptions) (*DcpClient, error) {
	if opts.ConnectionName == "" {
		return nil, errors.New("connection name must be specified")
	}
	if opts.SelectedBucket == "" {
		return nil, errors.New("bucket name must be specified")
	}
	if (opts.ConnectionFlags & memdx.DcpConnectionFlagsProducer) == 0 {
		return nil, errors.New("dcp client only supports producer mode")
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	dcpCli := &DcpClient{
		logger:         logger,
		selectedBucket: opts.SelectedBucket,
		handlers:       opts.Handlers,
	}

	logger.Debug("id assigned for " + opts.Address)

	bootstrapHello := &memdx.HelloRequest{
		ClientName: []byte(opts.ClientName),
		RequestedFeatures: []memdx.HelloFeature{
			memdx.HelloFeatureDatatype,
			memdx.HelloFeatureSeqNo,
			memdx.HelloFeatureXattr,
			memdx.HelloFeatureXerror,
			memdx.HelloFeatureSnappy,
			memdx.HelloFeatureJSON,
			memdx.HelloFeatureDurations,
			memdx.HelloFeaturePreserveExpiry,
			memdx.HelloFeatureSyncReplication,
			memdx.HelloFeatureReplaceBodyWithXattr,
			memdx.HelloFeatureSelectBucket,
			memdx.HelloFeatureCreateAsDeleted,
			memdx.HelloFeatureAltRequests,
			memdx.HelloFeatureCollections,
		},
	}

	var bootstrapAuth *memdx.SaslAuthAutoOptions
	if opts.Authenticator != nil {
		username, password, err := opts.Authenticator.GetCredentials(ServiceTypeMemd, opts.Address)
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

	bootstrapSelectBucket := &memdx.SelectBucketRequest{
		BucketName: opts.SelectedBucket,
	}

	memdxClientOpts := &memdx.ClientOptions{
		UnsolicitedHandler: dcpCli.handleUnsolicitedPacket,
		OrphanHandler:      dcpCli.handleOrphanResponse,
		CloseHandler:       nil,
		Logger:             logger,
	}
	if opts.NewMemdxClient == nil {
		conn, err := memdx.DialConn(ctx, opts.Address, &memdx.DialConnOptions{TLSConfig: opts.TlsConfig})
		if err != nil {
			return nil, err
		}

		dcpCli.cli = memdx.NewClient(conn, memdxClientOpts)
	} else {
		dcpCli.cli = opts.NewMemdxClient(memdxClientOpts)
	}

	closeConnection := func() {
		if closeErr := dcpCli.Close(); closeErr != nil {
			dcpCli.logger.Debug("failed to close connection for DcpClient", zap.Error(closeErr))
		}
	}

	dcpCli.logger.Debug("bootstrapping")
	res, err := dcpCli.bootstrap(ctx, &memdx.BootstrapOptions{
		Hello:            bootstrapHello,
		GetErrorMap:      nil,
		Auth:             bootstrapAuth,
		SelectBucket:     bootstrapSelectBucket,
		GetClusterConfig: nil,
	})
	if err != nil {
		dcpCli.logger.Debug("bootstrap failed", zap.Error(err))
		closeConnection()

		return nil, contextualError{
			Message: "failed to bootstrap",
			Cause:   err,
		}
	}

	if res.Hello != nil {
		dcpCli.supportedFeatures = res.Hello.EnabledFeatures
	}

	dcpCli.logger.Debug("successfully bootstrapped new DcpClient",
		zap.Stringers("features", dcpCli.supportedFeatures))

	_, err = dcpCli.dcpOpenConnection(ctx, &memdx.DcpOpenConnectionRequest{
		ConnectionName: opts.ConnectionName,
		ConsumerName:   opts.ConsumerName,
		Flags:          opts.ConnectionFlags,
	})
	if err != nil {
		closeConnection()
		return nil, contextualError{
			Message: "dcp openconnection failed",
			Cause:   err,
		}
	}

	_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
		Key:   "send_stream_end_on_client_close_stream",
		Value: "true",
	})
	if err != nil {
		dcpCli.logger.Debug("failed to enable stream-end-on-close feature", zap.Error(err))
	} else {
		dcpCli.streamEndOnCloseEnabled = true
	}

	if opts.Priority != "" {
		_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "set_priority",
			Value: opts.Priority,
		})
		closeConnection()
		return nil, contextualError{
			Message: "dcp set_priority failed",
			Cause:   err,
		}
	}

	if opts.EnableStreamIds {
		_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "enable_stream_id",
			Value: "true",
		})
		if err != nil {
			dcpCli.logger.Debug("failed to enable stream-ids feature", zap.Error(err))
		} else {
			dcpCli.streamIdsEnabled = true
		}
	}

	if opts.ForceValueCompression {
		_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "force_value_compression",
			Value: "true",
		})
		if err != nil {
			dcpCli.logger.Debug("failed to enable forced value compression", zap.Error(err))
		}
	}

	if opts.EnableExpiryEvents {
		_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "enable_expiry_opcode",
			Value: "true",
		})
		if err != nil {
			dcpCli.logger.Debug("failed to enable expiry events feature", zap.Error(err))
		}
	}

	if opts.NoopInterval > 0 {
		_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
			Key:   "set_noop_interval",
			Value: fmt.Sprintf("%d", opts.NoopInterval/time.Second),
		})
		if err == nil {
			_, err = dcpCli.dcpControl(ctx, &memdx.DcpControlRequest{
				Key:   "enable_noop",
				Value: "true",
			})
		}
		if err != nil {
			dcpCli.logger.Debug("noop requested, but could not be enabled", zap.Error(err))
		} else {
			dcpCli.noopEnabled = true
		}
	}

	dcpCli.logger.Debug("successfully configured new DcpClient")

	return dcpCli, nil
}

func (c *DcpClient) HasFeature(feat memdx.HelloFeature) bool {
	return slices.Contains(c.supportedFeatures, feat)
}

func (c *DcpClient) StreamIdsEnabled() bool {
	return c.streamIdsEnabled
}

func (c *DcpClient) StreamEndOnCloseEnabled() bool {
	return c.streamEndOnCloseEnabled
}

func (c *DcpClient) SelectedBucket() string {
	return c.selectedBucket
}

func (c *DcpClient) RemoteAddr() net.Addr {
	return c.cli.RemoteAddr()
}

func (c *DcpClient) LocalAddr() net.Addr {
	return c.cli.LocalAddr()
}

func (c *DcpClient) WritePacket(pak *memdx.Packet) error {
	return c.cli.WritePacket(pak)
}

func (c *DcpClient) Dispatch(pak *memdx.Packet, cb memdx.DispatchCallback) (memdx.PendingOp, error) {
	return c.cli.Dispatch(pak, cb)
}

func (c *DcpClient) Close() error {
	c.logger.Info("closing")
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.logger.Debug("already closed")
		return nil
	}

	return c.cli.Close()
}

func (c *DcpClient) handleUnsolicitedPacket(pak *memdx.Packet) {
	err := memdx.UnsolicitedOpsParser{
		CollectionsEnabled: c.HasFeature(memdx.HelloFeatureCollections),
	}.Handle(c, pak, &memdx.UnsolicitedOpsHandlers{
		DcpSnapshotMarker:     c.handlers.DcpSnapshotMarker,
		DcpMutation:           c.handlers.DcpMutation,
		DcpDeletion:           c.handlers.DcpDeletion,
		DcpExpiration:         c.handlers.DcpExpiration,
		DcpCollectionCreation: c.handlers.DcpCollectionCreation,
		DcpCollectionDeletion: c.handlers.DcpCollectionDeletion,
		DcpCollectionFlush:    c.handlers.DcpCollectionFlush,
		DcpScopeCreation:      c.handlers.DcpScopeCreation,
		DcpScopeDeletion:      c.handlers.DcpScopeDeletion,
		DcpCollectionChanged:  c.handlers.DcpCollectionChanged,
		DcpStreamEnd:          c.handlers.DcpStreamEnd,
		DcpOSOSnapshot:        c.handlers.DcpOSOSnapshot,
		DcpSeqNoAdvanced:      c.handlers.DcpSeqNoAdvanced,
		DcpNoOp: func(evt *memdx.DcpNoOpEvent) (*memdx.DcpNoOpEventResponse, error) {
			return &memdx.DcpNoOpEventResponse{}, nil
		},
	})
	if err != nil {
		c.logger.Info("error handling unsolicited packet",
			zap.Error(err))
	}
}

func (c *DcpClient) handleOrphanResponse(pak *memdx.Packet) {
	c.logger.Info(
		"orphaned response encountered",
		zap.String("opaque", strconv.Itoa(int(pak.Opaque))),
		zap.String("opcode", pak.OpCode.String(pak.Magic)),
	)
}
