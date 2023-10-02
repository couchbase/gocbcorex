package gocbcorex

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

type StreamFunction[StreamT any] func(
	ctx context.Context,
	logger *zap.Logger,
	httpRoundTripper http.RoundTripper,
	endpoint string,
	userAgent string,
	authenticator Authenticator,
	stream chan<- StreamT,
) error

// StreamWatcher describes a type for watching streams on the cluster.
type StreamWatcher[StreamT any] interface {
	Watch(ctx context.Context, execFn StreamFunction[StreamT]) <-chan StreamT
}
