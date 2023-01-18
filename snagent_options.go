package core

import "crypto/tls"

// Temporary options.
type FakeAgentOptions struct {
	TLSConfig  *tls.Config
	BucketName string
	Username   string
	Password   string

	HTTPAddrs []string
	MemdAddrs []string
}
