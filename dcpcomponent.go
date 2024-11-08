package gocbcorex

import (
	"go.uber.org/zap"
)

type DcpComponentConfig struct {
	Endpoints     map[string]string
	Authenticator Authenticator
}

type DcpComponentOptions struct {
	Logger        *zap.Logger
	VbucketRouter VbucketRouter
	ClientManager *DcpStreamClientManager
}

type DcpComponent struct {
	logger        *zap.Logger
	vbs           VbucketRouter
	clientManager *DcpStreamClientManager
}

func NewDcpComponent(config *DcpComponentConfig, opts *DcpComponentOptions) *DcpComponent {
	return &DcpComponent{
		logger:        opts.Logger,
		vbs:           opts.VbucketRouter,
		clientManager: opts.ClientManager,
	}
}

func (c *DcpComponent) NewStreamGroup() (*DcpStreamGroup, error) {
	return &DcpStreamGroup{
		vbs:           c.vbs,
		clientManager: c.clientManager,
	}, nil
}
