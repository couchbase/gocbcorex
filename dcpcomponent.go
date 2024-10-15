package gocbcorex

import (
	"context"

	"go.uber.org/zap"
)

type DcpComponent struct {
	logger      *zap.Logger
	retries     RetryManager
	connManager KvClientManager
	vbs         VbucketRouter
}

func OrchestrateSimpleDcp[RespT any](
	ctx context.Context,
	rs RetryManager,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	nkcp KvClientManager,
	vbId uint16,
	fn func(endpoint string, client KvClient) (RespT, error),
) (RespT, error) {
	return OrchestrateRetries(
		ctx, rs,
		func() (RespT, error) {
			return OrchestrateMemdEndpoint(ctx, vb, ch, vbId, func(endpoint string) (RespT, error) {
				return OrchestrateMemdClient(ctx, nkcp, endpoint, func(client KvClient) (RespT, error) {
					return fn(endpoint, client)
				})
			})
		})
}

type DcpStreamAddFilterOptions struct {
	ScopeID       uint32
	CollectionIDs []uint32
}

type DcpStreamAddStreamOptions struct {
	StreamID uint16
}

type DcpStreamAddManifestOptions struct {
	ManifestUID uint64
}

type DcpStreamAddOptions struct {
	VbID           uint16
	VbUuid         uint64
	StartSeqNo     uint64
	EndSeqNo       uint64
	SnapStartSeqNo uint64
	SnapEndSeqNo   uint64

	DiskOnly     bool
	ActiveOnly   bool
	StrictVbUuid bool

	FilterOptions   *DcpStreamAddFilterOptions
	StreamOptions   *DcpStreamAddStreamOptions
	ManifestOptions *DcpStreamAddManifestOptions
}

type DcpStream interface {
	NextEvent() (DcpEvent, error)
	Close() error
}

func (d *DcpComponent) DcpOpenStream(ctx context.Context, opts DcpStreamAddOptions) (DcpStream, error) {
	return OrchestrateSimpleDcp(ctx, d.retries, d.vbs, nil, d.connManager, opts.VbID, func(endpoint string, client KvClient) (DcpStream, error) {

		return nil, nil
	})
}
