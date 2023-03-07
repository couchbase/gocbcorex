package gocbcorex

import "context"

type ConfigWatcher interface {
	Watch(ctx context.Context) <-chan *ParsedConfig
}
