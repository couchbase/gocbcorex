package gocbcorex

import (
	"context"
	"sync"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

type DcpComponent struct {
	logger *zap.Logger
	vbs    VbucketRouter
}

type dcpStreamGroupClient struct {
	cli *DcpClient

	// we need to track what streams are on this client, both for disambiguation
	// of various stream-ids, but also for cleanup of streams when the client
	// gets disconnected.
	streams []*DcpStream
}

type DcpStreamGroup struct {
	parent *DcpComponent

	lock    sync.Mutex
	clients map[string][]*dcpStreamGroupClient
}

func (c *DcpComponent) NewStreamGroup() (*DcpStreamGroup, error) {
	return &DcpStreamGroup{
		parent: c,
	}, nil
}

func (c *DcpStreamGroup) getClientForNewStream(vbId uint16) (*DcpClient, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	endpoint, err := c.parent.vbs.DispatchToVbucket(vbId)
	if err != nil {
		return nil, err
	}

	clients := c.clients[endpoint]
	if len(clients) == 0 {
		// need to create a client
	}

	if len(clients) == 1 {
		// 1 client, maybe can use it with stream-ids?
	}

	// multiple clients, need to make a new one

	return nil, nil
}

type DcpStream struct {
	parent   *DcpStreamGroup
	cli      *DcpClient
	vbId     uint16
	streamId uint16
}

func (g *DcpStreamGroup) NewStream(vbId uint16, handlers DcpEventsHandlers) (*DcpStream, error) {
	return &DcpStream{
		parent: g,
	}, nil
}

func (s *DcpStream) Close(ctx context.Context) error {
	_, err := s.cli.DcpCloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: s.vbId,
		StreamId:  s.streamId,
	})
	if err != nil {
		return err
	}

	return nil
}
