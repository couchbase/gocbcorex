package cbauthx

type RevRpcHandlersFnsImpl struct {
	HeartbeatFn   func(opts *HeartbeatOptions) (bool, error)
	UpdateDBFn    func(opts *UpdateDBOptions) (bool, error)
	UpdateDBExtFn func(opts *UpdateDBExtOptions) (bool, error)
}

var _ RevRpcHandlers = (*RevRpcHandlersFnsImpl)(nil)

func (h RevRpcHandlersFnsImpl) Heartbeat(opts *HeartbeatOptions) (bool, error) {
	if h.HeartbeatFn != nil {
		return h.HeartbeatFn(opts)
	}
	return false, nil
}

func (h RevRpcHandlersFnsImpl) UpdateDB(opts *UpdateDBOptions) (bool, error) {
	if h.UpdateDBFn != nil {
		return h.UpdateDBFn(opts)
	}
	return false, nil
}

func (h RevRpcHandlersFnsImpl) UpdateDBExt(opts *UpdateDBExtOptions) (bool, error) {
	if h.UpdateDBExtFn != nil {
		return h.UpdateDBExtFn(opts)
	}
	return false, nil
}
