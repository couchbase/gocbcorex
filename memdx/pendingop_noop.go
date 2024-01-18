package memdx

type PendingOpNoop struct {
}

func (p PendingOpNoop) Cancel(_ error) {
}
