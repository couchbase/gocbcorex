package gocbcorex

import (
	"context"

	"go.uber.org/zap"
)

type HTTPComponent struct {
	logger  *zap.Logger
	httpMgr HTTPClientManager
	retries RetryManager
}

func (hc *HTTPComponent) SendHTTPRequest(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error) {
	client, err := hc.httpMgr.GetClient()
	if err != nil {
		return nil, err
	}

	return client.Do(ctx, req)
}
