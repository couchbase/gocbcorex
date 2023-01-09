package core

type VbucketDispatcher interface {
	DispatchByKey(ctx *AsyncContext, key []byte) (string, error)
	DispatchToVbucket(ctx *AsyncContext, vbID uint16) (string, error)
	StoreVbucketRoutingInfo(info *vbucketRoutingInfo)
}

type vbucketRoutingInfo struct {
	vbmap      *vbucketMap
	serverList []string
}

type vbucketDispatcher struct {
	routingInfo AtomicPointer[vbucketRoutingInfo]
}

func (vbd *vbucketDispatcher) StoreVbucketRoutingInfo(info *vbucketRoutingInfo) {
	vbd.storeRoutingInfo(info)
}

func (vbd *vbucketDispatcher) DispatchByKey(ctx *AsyncContext, key []byte) (string, error) {
	info := vbd.loadRoutingInfo()
	if info == nil {
		return "", placeholderError{"imnotgood"}
	}

	idx, err := info.vbmap.NodeByKey(key, 0)
	if err != nil {
		return "", err
	}

	// TODO: This really shouldn't be possible, and should possibly also be a panic condition?
	if idx > len(info.serverList) {
		return "", placeholderError{"imnotgood"}
	}

	return info.serverList[idx], nil
}

func (vbd *vbucketDispatcher) DispatchToVbucket(ctx *AsyncContext, vbID uint16) (string, error) {
	info := vbd.loadRoutingInfo()
	if info == nil {
		return "", placeholderError{"imnotgood"}
	}

	idx, err := info.vbmap.NodeByVbucket(vbID, 0)
	if err != nil {
		return "", err
	}

	if idx > len(info.serverList) {
		return "", placeholderError{"imnotgood"}
	}

	return info.serverList[idx], nil
}

func (vbd *vbucketDispatcher) loadRoutingInfo() *vbucketRoutingInfo {
	return vbd.routingInfo.Load()
}

func (vbd *vbucketDispatcher) storeRoutingInfo(new *vbucketRoutingInfo) {
	vbd.routingInfo.Store(new)
}
