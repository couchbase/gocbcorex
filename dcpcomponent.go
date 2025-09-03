package gocbcorex

import "go.uber.org/zap"

type DcpStreamSetManager struct {
	Logger      *zap.Logger
	NmvHandler  NotMyVbucketConfigHandler
	Retries     RetryManager
	Vbs         VbucketRouter
	SsClientMgr DcpStreamSetClientManager
}
