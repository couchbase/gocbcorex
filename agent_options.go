package gocbcorex

import (
	"crypto/tls"
	"time"

	"go.uber.org/zap"
)

type AgentReconfigureOptions struct {
	TLSConfig     *tls.Config
	Authenticator Authenticator
	BucketName    string
}

// Temporary options.
type AgentOptions struct {
	Logger *zap.Logger

	TLSConfig     *tls.Config
	Authenticator Authenticator
	BucketName    string

	SeedConfig SeedConfig

	CompressionConfig CompressionConfig

	ConfigPollerConfig ConfigPollerConfig

	HTTPConfig HTTPConfig
}

// SeedConfig specifies initial seed configuration options such as addresses.
type SeedConfig struct {
	HTTPAddrs []string
	MemdAddrs []string
}

// CompressionConfig specifies options for controlling compression applied to documents using KV.
type CompressionConfig struct {
	EnableCompression    bool
	DisableDecompression bool
	MinSize              int
	MinRatio             float64
}

// ConfigPollerConfig specifies options for controlling the cluster configuration pollers.
type ConfigPollerConfig struct {
	HTTPRedialPeriod time.Duration
	HTTPRetryDelay   time.Duration
	HTTPMaxWait      time.Duration
	// CccpMaxWait      time.Duration
	// CccpPollPeriod   time.Duration
}

// HTTPConfig specifies http related configuration options.
type HTTPConfig struct {
	// MaxIdleConns controls the maximum number of idle (keep-alive) connections across all hosts.
	MaxIdleConns int
	// MaxIdleConnsPerHost controls the maximum idle (keep-alive) connections to keep per-host.
	MaxIdleConnsPerHost int
	ConnectTimeout      time.Duration
	// IdleConnectionTimeout is the maximum amount of time an idle (keep-alive) connection will remain idle before
	// closing itself.
	IdleConnectionTimeout time.Duration
}
