package gocbcorex

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type DcpStreamGroupOptions struct {
	Logger                 *zap.Logger
	NewDcpClientProviderFn NewDcpStreamClientProviderFunc
	VbucketRouter          VbucketRouter

	Bucket                string
	ConnectionName        string
	ConsumerName          string
	ConnectionFlags       memdx.DcpConnectionFlags
	NoopInterval          time.Duration
	BufferSize            int
	Priority              string
	ForceValueCompression bool
	EnableCursorDropping  bool
	EnableExpiryEvents    bool
	EnableOso             bool
	EnableSeqNoAdvance    bool
	BackfillOrder         string
	EnableChangeStreams   bool
}

type DcpStreamGroup struct {
	logger        *zap.Logger
	vbs           VbucketRouter
	clientManager *DcpStreamClientManager

	lock    sync.Mutex
	clients map[string]*DcpStreamClient
}

func (c *DcpStreamGroup) GetClient(ctx context.Context, vbId uint16) (*DcpStreamClient, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	endpoint, err := c.vbs.DispatchToVbucket(vbId)
	if err != nil {
		return nil, err
	}

	cli := c.clients[endpoint]
	if cli != nil {
		// if there is already a client for this endpoint, we can just return it.
		return cli, nil
	}

	return nil, nil
}

func (g *DcpStreamGroup) OpenStream(ctx context.Context, opts *memdx.DcpStreamReqRequest) (DcpStream, error) {
	g.logger.Debug("openening stream",
		zap.Uint16("vbucket", opts.VbucketID))

	cli, err := g.GetClient(ctx, opts.VbucketID)
	if err != nil {
		return nil, err
	}

	return cli.OpenStream(ctx, opts)
}
