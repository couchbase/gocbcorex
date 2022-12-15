package core

import "github.com/couchbase/gocbcore/v10/memd"

type ServerDispatcher interface {
	DispatchToServer(endpoint string, pak *memd.Packet, cb func(*memd.Packet, error))
}
