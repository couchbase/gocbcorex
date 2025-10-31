package memdx

type clientPendingOp struct {
	client   *Client
	opaqueID uint32
}

func (po clientPendingOp) Cancel(err error) bool {
	return po.client.cancelOp(po.opaqueID, err)
}
