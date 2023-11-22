package gocbcorex

import (
	"crypto/tls"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"

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

	RetryManager RetryManager

	HTTPConfig HTTPConfig
}

func (opts AgentOptions) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("bucket-name", opts.BucketName)
	if err := enc.AddObject("seed-config", opts.SeedConfig); err != nil {
		return err
	}
	if err := enc.AddObject("compression-config", opts.CompressionConfig); err != nil {
		return err
	}
	if err := enc.AddObject("poller-config", opts.ConfigPollerConfig); err != nil {
		return err
	}
	if err := enc.AddObject("http-config", opts.HTTPConfig); err != nil {
		return err
	}

	return nil
}

// SeedConfig specifies initial seed configuration options such as addresses.
type SeedConfig struct {
	HTTPAddrs []string
	MemdAddrs []string
}

func (c SeedConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("http-addresses", strings.Join(c.HTTPAddrs, ","))
	enc.AddString("memd-addresses", strings.Join(c.MemdAddrs, ","))

	return nil
}

// CompressionConfig specifies options for controlling compression applied to documents using KV.
type CompressionConfig struct {
	EnableCompression    bool
	DisableDecompression bool
	MinSize              int
	MinRatio             float64
}

func (c CompressionConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddBool("enabled", c.EnableCompression)
	enc.AddBool("decompression-disabled", c.DisableDecompression)
	enc.AddInt("min-size", c.MinSize)
	enc.AddFloat64("min-ratio", c.MinRatio)

	return nil
}

// ConfigPollerConfig specifies options for controlling the cluster configuration pollers.
type ConfigPollerConfig struct {
	HTTPRedialPeriod time.Duration
	HTTPRetryDelay   time.Duration
	HTTPMaxWait      time.Duration
	// CccpMaxWait      time.Duration
	// CccpPollPeriod   time.Duration
}

func (c ConfigPollerConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("redial-period", c.HTTPRedialPeriod.String())
	enc.AddString("retry-delay", c.HTTPRetryDelay.String())
	enc.AddString("max-wait", c.HTTPMaxWait.String())

	return nil
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

func (c HTTPConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("max-idle-conns", c.MaxIdleConns)
	enc.AddInt("max-idle-conns-per-host", c.MaxIdleConnsPerHost)
	enc.AddString("connection-timeout", c.ConnectTimeout.String())
	enc.AddString("idle-connection-timeout", c.IdleConnectionTimeout.String())

	return nil
}
