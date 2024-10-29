package memdx

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type UnsolicitedOpsHandlers struct {
	ClustermapChange func(req *ClustermapChangeEvent) error
}

type UnsolicitedOpsParser struct {
}

func (o UnsolicitedOpsParser) parseSrvClustermapChangeNotification(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.ClustermapChange == nil {
		return errors.New("unhandled ClustermapChange event")
	}

	if len(pak.Extras) == 4 {
		return handlers.ClustermapChange(&ClustermapChangeEvent{
			RevEpoch:   0,
			Rev:        int64(binary.BigEndian.Uint32(pak.Extras[0:])),
			BucketName: pak.Key,
			Config:     pak.Value,
		})
	} else if len(pak.Extras) == 16 {
		return handlers.ClustermapChange(&ClustermapChangeEvent{
			RevEpoch:   int64(binary.BigEndian.Uint64(pak.Extras[0:])),
			Rev:        int64(binary.BigEndian.Uint64(pak.Extras[8:])),
			BucketName: pak.Key,
			Config:     pak.Value,
		})
	}

	return &protocolError{"unknown clustermap change notification format"}
}

func (o UnsolicitedOpsParser) Handle(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if pak.Magic.IsRequest() {
		if pak.Magic.IsServerInitiated() {
			if pak.OpCode == OpCodeSrvClustermapChangeNotification {
				return o.parseSrvClustermapChangeNotification(pak, handlers)
			}
		}
	}

	return &protocolError{
		fmt.Sprintf("unknown unsolicited event (opcode: %s)",
			pak.OpCode.String(pak.Magic))}
}
