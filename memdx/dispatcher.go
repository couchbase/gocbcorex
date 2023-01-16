package memdx

type DispatchCallback func(*Packet, error) bool

type Dispatcher interface {
	Dispatch(*Packet, DispatchCallback) error
}
