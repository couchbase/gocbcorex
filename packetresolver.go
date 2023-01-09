package core

import "github.com/couchbase/gocbcore/v10/memd"

type PacketResolver interface {
	ResolvePacket(*memd.Packet) error
}
