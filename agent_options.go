package core

import (
	"crypto/tls"

	"go.uber.org/zap"
)

// Temporary options.
type AgentOptions struct {
	Logger *zap.Logger

	TLSConfig  *tls.Config
	BucketName string
	Username   string
	Password   string

	HTTPAddrs []string
	MemdAddrs []string
}

type AgentReconfigureOptions struct {
	TLSConfig  *tls.Config
	BucketName string
	Username   string
	Password   string
}
