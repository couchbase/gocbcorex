package core

import (
	"crypto/tls"

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
}

// SeedConfig specifies initial seed configuration options such as addresses.
type SeedConfig struct {
	HTTPAddrs []string
	MemdAddrs []string
}
