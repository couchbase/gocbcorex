package memdx

type clientPendingOp struct {
	client   *Client
	opaqueID uint32
}

func (po clientPendingOp) Cancel(err error) {
	go po.client.cancelHandler(po.opaqueID, err)
}
