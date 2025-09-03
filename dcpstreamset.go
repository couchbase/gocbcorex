package gocbcorex

import "go.uber.org/zap"

type DcpStreamSet struct {
	logger     *zap.Logger
	nmvHandler NotMyVbucketConfigHandler
	retries    RetryManager
	vbs        VbucketRouter

	// this is specific to this stream set
	clientMgr DcpEndpointClientManager
}

func (d *DcpStreamSet) Close() error {
	return d.clientMgr.Close()
}
