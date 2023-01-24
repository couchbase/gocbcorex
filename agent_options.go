package core

import "crypto/tls"

// Temporary options.
type AgentOptions struct {
	TLSConfig  *tls.Config
	BucketName string
	Username   string
	Password   string

	HTTPAddrs []string
	MemdAddrs []string
}
