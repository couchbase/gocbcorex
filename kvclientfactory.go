package gocbcorex

import "context"

type kvClientFactory interface {
	NewKvClient(ctx context.Context, config *KvClientConfig) (KvClient, error)
}
