package memdx

type clientPendingOp struct {
	client   *Client
	opaqueID uint32
}

func (po clientPendingOp) Cancel() bool {
	return po.client.cancelHandler(po.opaqueID)
}
