package gocbcorex

import (
	"context"
	"errors"
	"sync"

	"github.com/couchbase/gocbcorex/memdx"
)

var (
	ErrDcpVbucketAlreadyOpen = errors.New("vbucket already open")
	ErrDcpVbucketNotOpen     = errors.New("vbucket not open")
)

type dcpAgentStreamSetState struct {
	vbuckets map[uint16]*DcpClient
}

type DcpAgentStreamSet struct {
	streamId uint64
	openCb   func(resp *memdx.DcpStreamReqResponse)

	vbRouter    VbucketRouter
	collections CollectionResolver
	clientMgr   DcpClientManager

	lock  sync.Mutex
	state dcpAgentStreamSetState
}

func newDcpStreamSet(
	streamId uint64,
	vbRouter VbucketRouter,
	collections CollectionResolver,
	clientMgr DcpClientManager,
	openCb func(resp *memdx.DcpStreamReqResponse),
) (*DcpAgentStreamSet, error) {
	return &DcpAgentStreamSet{
		streamId: streamId,
		openCb:   openCb,

		vbRouter:    vbRouter,
		collections: collections,
		clientMgr:   clientMgr,

		state: dcpAgentStreamSetState{
			vbuckets: make(map[uint16]*DcpClient),
		},
	}, nil
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

func (s *DcpAgentStreamSet) OpenVbucket(
	ctx context.Context,
	opts *OpenVbucketOptions,
) (*memdx.DcpStreamReqResponse, error) {
	endpoint, err := s.vbRouter.DispatchToVbucket(opts.VbucketId, 0)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	if s.state.vbuckets[opts.VbucketId] != nil {
		s.lock.Unlock()
		return nil, ErrDcpVbucketAlreadyOpen
	}

	cli, err := s.clientMgr.GetClient(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	s.state.vbuckets[opts.VbucketId] = cli
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
	}, s.openCb)
	if err != nil {
		s.lock.Lock()
		delete(s.state.vbuckets, opts.VbucketId)
		s.lock.Unlock()

		return nil, err
	}

	return resp, nil
}

type CloseVbucketOptions struct {
	VbucketId uint16
}

func (s *DcpAgentStreamSet) CloseVbucket(ctx context.Context, vbucketId uint16) error {
	s.lock.Lock()
	vbCli := s.state.vbuckets[vbucketId]
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

	s.state.vbuckets[vbucketId] = nil
	s.lock.Unlock()

	return nil
}
