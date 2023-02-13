package memdx

type PendingOp interface {
	Cancel() bool
}

type DispatchCallback func(*Packet, error) bool

type Dispatcher interface {
	Dispatch(*Packet, DispatchCallback) (PendingOp, error)
	LocalAddr() string
	RemoteAddr() string
}
