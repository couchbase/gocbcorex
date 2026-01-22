package gocbcorex

import (
	"errors"
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

type NewStreamSetOptions struct {
	NoopInterval time.Duration
	DcpOpts      KvClientDcpOptions
	Handlers     DcpEventsHandlers
}

func (m *DcpStreamSetManager) NewStreamSet(opts NewStreamSetOptions) (*DcpStreamSet, error) {
	bucketName := m.BucketName
	if bucketName == "" {
		return nil, errors.New("cannot create DCP stream set without a selected bucket")
	}

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

func (m *DcpStreamSetManager) Reconfigure(bucketName string) error {
	// We rely on the fact that it is illegal to call NewStreamSet before a bucket
	// has been selected on the agent.
	m.BucketName = bucketName

	return nil
}
