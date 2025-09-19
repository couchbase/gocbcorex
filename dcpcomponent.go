package gocbcorex

import (
	"time"

	"go.uber.org/zap"
)

type DcpStreamSetManager struct {
	Logger     *zap.Logger
	NmvHandler NotMyVbucketConfigHandler
	Retries    RetryManager
	Vbs        VbucketRouter
	MconnMgr   MultiKvEndpointClientManager
	BucketName string
}

/*
	StreamOptions: gocbcorex.DcpStreamOptions{
		ConnectionName: "test-conn",
		NoopInterval:   5 * time.Second,
	},
*/

type NewStreamSetOptions struct {
	NoopInterval time.Duration
	DcpOpts      KvClientDcpOptions
	Handlers     DcpEventsHandlers
}

func (m *DcpStreamSetManager) NewStreamSet(opts NewStreamSetOptions) (*DcpStreamSet, error) {
	return NewDcpStreamSet(
		m.Logger,
		m.NmvHandler,
		m.Retries,
		m.Vbs,
		m.MconnMgr,
		m.BucketName,
		&opts.DcpOpts,
		opts.Handlers,
	)
}
