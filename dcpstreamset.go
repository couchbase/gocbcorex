package gocbcorex

import (
	"context"
	"errors"
	"sync"

	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

var (
	ErrDcpVbucketAlreadyOpen = errors.New("vbucket already open")
	ErrDcpVbucketNotOpen     = errors.New("vbucket not open")
)

type DcpStreamSet struct {
	logger     *zap.Logger
	nmvHandler NotMyVbucketConfigHandler
	retries    RetryManager
	vbs        VbucketRouter

	clientMgr KvEndpointClientManager
	streamId  uint64
	openCb    func(resp *memdx.DcpStreamReqResponse)

	lock     sync.Mutex
	vbuckets map[uint16]KvClient
}

func NewDcpStreamSet(
	logger *zap.Logger,
	nmvHandler NotMyVbucketConfigHandler,
	retries RetryManager,
	vbs VbucketRouter,
	mconnMgr MultiKvEndpointClientManager,
	bucketName string,
	dcpOpts *KvClientDcpOptions,
	handlers DcpEventsHandlers,
) (*DcpStreamSet, error) {
	streamRouter := &DcpStreamRouterStatic{
		ClientHandlers: handlers,
	}

	clientMgr, err := mconnMgr.NewManager(NewManagerOptions{
		NumPoolConnections: 1,
		OnDemandConnect:    true,
		SelectedBucket:     bucketName,
		BootstrapOpts: KvClientBootstrapOptions{
			DisableOutOfOrderExec: true,
		},
		DcpOpts:     dcpOpts,
		DcpHandlers: streamRouter.Handlers(),
	})
	if err != nil {
		return nil, err
	}

	return &DcpStreamSet{
		logger:     logger,
		nmvHandler: nmvHandler,
		retries:    retries,
		vbs:        vbs,
		clientMgr:  clientMgr,
		streamId:   0,
		openCb: func(resp *memdx.DcpStreamReqResponse) {
			if handlers.StreamOpen != nil {
				handlers.StreamOpen(resp)
			}
		},

		vbuckets: make(map[uint16]KvClient),
	}, nil
}

func (d *DcpStreamSet) Close() error {
	return d.clientMgr.Close()
}

type OpenVbucketOptions struct {
	VbucketId      uint16
	Flags          uint32
	StartSeqNo     uint64
	EndSeqNo       uint64
	VbUuid         uint64
	SnapStartSeqNo uint64
	SnapEndSeqNo   uint64

	ManifestUid   uint64
	ScopeId       *uint32
	CollectionIds []uint32
}

func (s *DcpStreamSet) OpenVbucket(
	ctx context.Context,
	opts *OpenVbucketOptions,
) (*memdx.DcpStreamReqResponse, error) {
	endpoint, err := s.vbs.DispatchToVbucket(opts.VbucketId, 0)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	if s.vbuckets[opts.VbucketId] != nil {
		s.lock.Unlock()
		return nil, ErrDcpVbucketAlreadyOpen
	}

	cli, err := s.clientMgr.GetEndpointClient(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	s.vbuckets[opts.VbucketId] = cli
	s.lock.Unlock()

	resp, err := cli.DcpStreamReq(ctx, &memdx.DcpStreamReqRequest{
		VbucketID:      opts.VbucketId,
		Flags:          opts.Flags,
		StartSeqNo:     opts.StartSeqNo,
		EndSeqNo:       opts.EndSeqNo,
		VbUuid:         opts.VbUuid,
		SnapStartSeqNo: opts.SnapStartSeqNo,
		SnapEndSeqNo:   opts.SnapEndSeqNo,

		ManifestUid:   opts.ManifestUid,
		StreamId:      s.streamId,
		ScopeId:       opts.ScopeId,
		CollectionIds: opts.CollectionIds,
	}, func(dsrr *memdx.DcpStreamReqResponse, err error) {
		if err == nil {
			s.openCb(dsrr)
		}
	})
	if err != nil {
		s.lock.Lock()
		delete(s.vbuckets, opts.VbucketId)
		s.lock.Unlock()

		return nil, err
	}

	return resp, nil
}

type CloseVbucketOptions struct {
	VbucketId uint16
}

func (s *DcpStreamSet) CloseVbucket(ctx context.Context, vbucketId uint16) error {
	s.lock.Lock()
	vbCli := s.vbuckets[vbucketId]
	if vbCli == nil {
		s.lock.Unlock()
		return ErrDcpVbucketNotOpen
	}

	_, err := vbCli.DcpCloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: vbucketId,
	})
	if err != nil {
		s.lock.Unlock()
		return err
	}

	delete(s.vbuckets, vbucketId)
	s.lock.Unlock()

	return nil
}
