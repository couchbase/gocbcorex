package gocbcorex

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type DcpClientConfig struct {
	Address        string
	TlsConfig      *tls.Config
	ClientName     string
	Authenticator  Authenticator
	SelectedBucket string

	BufferSize            int
	NoopInterval          time.Duration
	Priority              string
	ForceValueCompression bool
	EnableCursorDropping  bool
	EnableExpiryEvents    bool
	EnableOso             bool
	BackfillOrder         string
	EnableChangeStreams   bool
}

type DcpClientOptions struct {
	Logger         *zap.Logger
	NewMemdxClient GetMemdxClientFunc
	CloseHandler   func(KvClient, error)
}

type dcpClient struct {
	logger         *zap.Logger
	remoteHostName string
	remoteHost     string
	remotePort     int
	localHost      string
	localPort      int

	cli MemdxDispatcherCloser

	supportedFeatures []memdx.HelloFeature

	closed uint32
}

func NewDcpClient(ctx context.Context, config *DcpClientConfig, opts *DcpClientOptions) (*dcpClient, error) {
	parseHostPort := func(addr string) (string, int) {
		host, portStr, _ := net.SplitHostPort(addr)
		parsedPort, _ := strconv.ParseInt(portStr, 10, 64)
		return host, int(parsedPort)
	}

	if config.SelectedBucket == "" {
		return nil, errors.New("bucket name must be specified")
	}

	logger := loggerOrNop(opts.Logger)
	// We namespace the pool to improve debugging,
	logger = logger.With(
		zap.String("clientId", uuid.NewString()[:8]),
	)

	remoteHostName, remotePort := parseHostPort(config.Address)

	dcpCli := &dcpClient{
		remoteHostName: remoteHostName,
		remotePort:     remotePort,
		logger:         logger,
	}

	logger.Debug("id assigned for " + config.Address)

	bootstrapHello := &memdx.HelloRequest{
		ClientName: []byte(config.ClientName),
		RequestedFeatures: []memdx.HelloFeature{
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
		},
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

	bootstrapSelectBucket := &memdx.SelectBucketRequest{
		BucketName: config.SelectedBucket,
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

		dcpCli.cli = memdx.NewClient(conn, memdxClientOpts)
	} else {
		dcpCli.cli = opts.NewMemdxClient(memdxClientOpts)
	}

	remoteHost, _ := parseHostPort(dcpCli.cli.RemoteAddr())
	localHost, localPort := parseHostPort(dcpCli.cli.LocalAddr())
	dcpCli.remoteHost = remoteHost
	dcpCli.localHost = localHost
	dcpCli.localPort = localPort

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
		if closeErr := dcpCli.Close(); closeErr != nil {
			dcpCli.logger.Debug("failed to close connection for KvClient", zap.Error(closeErr))
		}

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

	return dcpCli, nil
}

func (c *dcpClient) Close() error {
	c.logger.Info("closing")
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.logger.Debug("already closed")
		return nil
	}

	return c.cli.Close()
}
