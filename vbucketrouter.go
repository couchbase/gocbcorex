package gocbcorex

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
)

var (
	ErrNoVbucketMap = contextualDeadline{"no vbucket map is available"}

	ErrNoForwardVbucketMap = errors.New("no fast-forward vbucket map is available")
)

type VbucketRouter interface {
	UpdateRoutingInfo(*VbucketRoutingInfo)
	DispatchByKey(key []byte, vbServerIdx uint32) (string, uint16, error)
	DispatchToVbucket(vbID uint16, vbServerIdx uint32) (string, error)
	DispatchToVbucketForward(vbID uint16, vbServerIdx uint32) (string, error)
	NumReplicas() (int, error)
	GetServerList() ([]string, error)
}

type VbucketRoutingInfo struct {
	VbMap *VbucketMap
	// VbMapForward is the fast-forward map, which is only present while a
	// rebalance is in progress.  It shares ServerList with VbMap.
	VbMapForward *VbucketMap
	ServerList   []string
}

type VbucketRouterOptions struct {
	Logger *zap.Logger
}

type vbucketRouter struct {
	logger      *zap.Logger
	routingInfo atomic.Pointer[VbucketRoutingInfo]
}

var _ VbucketRouter = (*vbucketRouter)(nil)

func NewVbucketRouter(opts *VbucketRouterOptions) *vbucketRouter {
	if opts == nil {
		opts = &VbucketRouterOptions{}
	}

	vbd := &vbucketRouter{
		logger: loggerOrNop(opts.Logger),
	}

	return vbd
}

func (vbd *vbucketRouter) UpdateRoutingInfo(info *VbucketRoutingInfo) {
	vbd.routingInfo.Store(info)
}

func (vbd *vbucketRouter) NumReplicas() (int, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return 0, err
	}

	return info.VbMap.NumReplicas(), nil
}

func (vbd *vbucketRouter) GetServerList() ([]string, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return nil, err
	}

	servers := make([]string, len(info.ServerList))
	copy(servers, info.ServerList)
	return servers, nil
}

func (vbd *vbucketRouter) getRoutingInfo() (*VbucketRoutingInfo, error) {
	routing := vbd.routingInfo.Load()
	if routing == nil {
		return nil, ErrNoVbucketMap
	}

	return routing, nil
}

func (vbd *vbucketRouter) DispatchByKey(key []byte, vbServerIdx uint32) (string, uint16, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return "", 0, err
	}

	vbID := info.VbMap.VbucketByKey(key)
	idx, err := info.VbMap.NodeByVbucket(vbID, vbServerIdx)
	if err != nil {
		return "", 0, err
	}

	if idx < 0 || idx >= len(info.ServerList) {
		return "", 0, noServerAssignedError{
			RequestedVbId: vbID,
		}
	}

	return info.ServerList[idx], vbID, nil
}

func (vbd *vbucketRouter) DispatchToVbucket(vbID uint16, vbServerIdx uint32) (string, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return "", err
	}

	return dispatchToVbucketWithMap(info.VbMap, info.ServerList, vbID, vbServerIdx)
}

// DispatchToVbucketForward returns the endpoint which the fast-forward map
// assigns to the vbucket.  This is where the vbucket will live once the
// in-progress rebalance completes.
func (vbd *vbucketRouter) DispatchToVbucketForward(vbID uint16, vbServerIdx uint32) (string, error) {
	info, err := vbd.getRoutingInfo()
	if err != nil {
		return "", err
	}

	if info.VbMapForward == nil {
		return "", ErrNoForwardVbucketMap
	}

	return dispatchToVbucketWithMap(info.VbMapForward, info.ServerList, vbID, vbServerIdx)
}

func dispatchToVbucketWithMap(vbMap *VbucketMap, serverList []string, vbID uint16, vbServerIdx uint32) (string, error) {
	idx, err := vbMap.NodeByVbucket(vbID, vbServerIdx)
	if err != nil {
		return "", err
	}

	if idx < 0 || idx >= len(serverList) {
		return "", noServerAssignedError{
			RequestedVbId: vbID,
		}
	}

	return serverList[idx], nil
}

// fastForwardEndpoint returns the endpoint the fast-forward map assigns to the
// vbucket, if there is one and it differs from the endpoint that rejected the
// request.
//
// During a rebalance there is a window after a vbucket has been taken over by
// its new owner, but before the server has published a vbucket map reflecting
// that.  In that window the old owner will reply NOT_MY_VBUCKET with a config
// which still names itself as owner, but the fast-forward map in that config
// names the new owner.
func fastForwardEndpoint(vb VbucketRouter, vbID uint16, vbServerIdx uint32, rejectedEndpoint string) (string, bool) {
	endpoint, err := vb.DispatchToVbucketForward(vbID, vbServerIdx)
	if err != nil || endpoint == rejectedEndpoint {
		return "", false
	}

	return endpoint, true
}

type NotMyVbucketConfigHandler interface {
	HandleNotMyVbucketConfig(config *cbconfig.TerseConfigJson, sourceHostname string)
}

func OrchestrateMemdRouting[RespT any](
	ctx context.Context,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	key []byte,
	vbServerIdx uint32,
	fn func(endpoint string, vbID uint16) (RespT, error),
) (RespT, error) {
	endpoint, vbID, err := vb.DispatchByKey(key, vbServerIdx)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	// once we have redirected a request using the fast-forward map, this holds
	// the endpoint which the main map pointed at (and which rejected it).
	fwdFromEndpoint := ""

	for {
		res, err := fn(endpoint, vbID)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				// if we have a config handler, lets try to parse the config and update
				if ch != nil {
					var nmvErr *memdx.ServerErrorWithConfig
					if errors.As(err, &nmvErr) {
						var kvCliErr *KvClientError
						if errors.As(err, &kvCliErr) {
							sourceHostname := kvCliErr.RemoteHostname

							config, parseErr := cbconfig.ParseTerseConfig(
								nmvErr.ConfigJson,
								sourceHostname)
							if parseErr != nil {
								return res, &VbucketMapOutdatedError{
									Cause: err,
								}
							}

							ch.HandleNotMyVbucketConfig(config, sourceHostname)
						}
					}
				}
			}

			if errors.Is(err, memdx.ErrNotMyVbucket) || errors.Is(err, memdx.ErrConfigOnly) {
				isNotMyVbucket := errors.Is(err, memdx.ErrNotMyVbucket)

				newEndpoint, newVbID, err := vb.DispatchByKey(key, vbServerIdx)
				if err != nil {
					var emptyResp RespT
					return emptyResp, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				if newEndpoint == endpoint && newVbID == vbID {
					// if the vbucket is being moved by a rebalance, the fast-forward
					// map may know where it has gone before the main map does, so
					// we try there (once) before giving up.
					if isNotMyVbucket && fwdFromEndpoint == "" {
						if fwdEndpoint, ok := fastForwardEndpoint(vb, vbID, vbServerIdx, endpoint); ok {
							fwdFromEndpoint = endpoint
							endpoint = fwdEndpoint
							continue
						}
					}

					// if after the update we are going to be sending the request back
					// to the place that rejected it, we consider this non-deterministic
					// and fall back to the application to deal with (or retries).
					return res, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				if fwdFromEndpoint != "" && newEndpoint == fwdFromEndpoint && newVbID == vbID {
					// the fast-forward owner rejected the request too, and the main
					// map still points at the original owner which already rejected it.
					return res, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				endpoint = newEndpoint
				vbID = newVbID
				continue
			}

			return res, err
		}

		return res, nil
	}
}

func OrchestrateMemdRoutingByVbucketId[RespT any](
	ctx context.Context,
	vb VbucketRouter,
	ch NotMyVbucketConfigHandler,
	vbID uint16,
	vbServerIdx uint32,
	fn func(endpoint string) (RespT, error),
) (RespT, error) {
	endpoint, err := vb.DispatchToVbucket(vbID, vbServerIdx)
	if err != nil {
		var emptyResp RespT
		return emptyResp, err
	}

	// once we have redirected a request using the fast-forward map, this holds
	// the endpoint which the main map pointed at (and which rejected it).
	fwdFromEndpoint := ""

	for {
		res, err := fn(endpoint)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				// if we have a config handler, lets try to parse the config and update
				if ch != nil {
					var nmvErr *memdx.ServerErrorWithConfig
					if errors.As(err, &nmvErr) {
						var kvCliErr *KvClientError
						if errors.As(err, &kvCliErr) {
							sourceHostname := kvCliErr.RemoteHostname

							config, parseErr := cbconfig.ParseTerseConfig(
								nmvErr.ConfigJson,
								sourceHostname)
							if parseErr != nil {
								return res, &VbucketMapOutdatedError{
									Cause: err,
								}
							}

							ch.HandleNotMyVbucketConfig(config, sourceHostname)
						}
					}
				}

				newEndpoint, err := vb.DispatchToVbucket(vbID, vbServerIdx)
				if err != nil {
					var emptyResp RespT
					return emptyResp, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				if newEndpoint == endpoint {
					// if the vbucket is being moved by a rebalance, the fast-forward
					// map may know where it has gone before the main map does, so
					// we try there (once) before giving up.
					if fwdFromEndpoint == "" {
						if fwdEndpoint, ok := fastForwardEndpoint(vb, vbID, vbServerIdx, endpoint); ok {
							fwdFromEndpoint = endpoint
							endpoint = fwdEndpoint
							continue
						}
					}

					// if after the update we are going to be sending the request back
					// to the place that rejected it, we consider this non-deterministic
					// and fall back to the application to deal with (or retries).
					return res, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				if fwdFromEndpoint != "" && newEndpoint == fwdFromEndpoint {
					// the fast-forward owner rejected the request too, and the main
					// map still points at the original owner which already rejected it.
					return res, &VbucketMapOutdatedError{
						Cause: err,
					}
				}

				endpoint = newEndpoint
				continue
			}

			return res, err
		}

		return res, nil
	}
}
