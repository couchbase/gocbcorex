package core

import "github.com/couchbase/gocbcore/v10/memd"

type ServerDispatcher interface {
	DispatchToServer(ctx *AsyncContext, endpoint string, pak *memd.Packet, cb func(*memd.Packet, error)) error
}
