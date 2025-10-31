package memdx

type PendingOp interface {
	Cancel(err error) bool
}

type DispatchCallback func(*Packet, error) bool

type Dispatcher interface {
	Dispatch(*Packet, DispatchCallback) (PendingOp, error)
}
