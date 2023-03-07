package memdx

type pendingOpNoop struct {
}

func (p pendingOpNoop) Cancel(_ error) {
}
