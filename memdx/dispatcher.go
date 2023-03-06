package memdx

type PendingOp interface {
	Cancel(err error)
}

type DispatchCallback func(*Packet, error) bool

type Dispatcher interface {
	Dispatch(*Packet, DispatchCallback) (PendingOp, error)
	LocalAddr() string
	RemoteAddr() string
}
