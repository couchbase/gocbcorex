package memdx

type pendingOpNoop struct {
}

func (p pendingOpNoop) Cancel() bool {
	// Since we aren't cancelling anything, we need to return false
	// to indicate that the cancellation failed.  This means that
	// code will end up blocking for longer than it should.
	return false
}
