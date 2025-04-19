package memdx

type PacketType uint

const (
	PacketTypeUnknown PacketType = iota
	PacketTypeReq
	PacketTypeRes
)

type Packet struct {
	IsResponse    bool
	OpCode        OpCode
	Datatype      uint8
	VbucketID     uint16 // Only valid for Req-type packets
	Status        Status // Only valid for Res-type packets
	Opaque        uint32
	Cas           uint64
	Extras        []byte
	Key           []byte
	Value         []byte
	FramingExtras []byte // Only valid for Ext-type packets
}
