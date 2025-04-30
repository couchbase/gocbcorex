package memdx

import "encoding/binary"

type OpsCtrl struct {
	ExtFramesEnabled   bool
	CollectionsEnabled bool
}

type DcpGetFailoverLogRequest struct {
	VbucketID uint16
}

type DcpFailoverEntry struct {
	VbUuid uint64
	SeqNo  uint64
}

type DcpGetFailoverLogResponse struct {
	Entries []DcpFailoverEntry
}

func (o OpsCtrl) DcpGetFailoverLog(
	d Dispatcher,
	req *DcpGetFailoverLogRequest,
	cb func(*DcpGetFailoverLogResponse, error),
) (PendingOp, error) {
	return d.Dispatch(&Packet{
		OpCode:    OpCodeDcpGetFailoverLog,
		VbucketID: req.VbucketID,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		decompErr := OpsCore{}.maybeDecompressPacket(resp)
		if decompErr != nil {
			cb(nil, decompErr)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		numEntries := len(resp.Value) / 16
		entries := make([]DcpFailoverEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = DcpFailoverEntry{
				VbUuid: binary.BigEndian.Uint64(resp.Value[i*16+0:]),
				SeqNo:  binary.BigEndian.Uint64(resp.Value[i*16+8:]),
			}
		}

		cb(&DcpGetFailoverLogResponse{
			Entries: entries,
		}, nil)
		return false
	})
}

type GetVbucketSeqNosRequest struct {
	VbucketState VbucketState
	CollectionID uint32
}

type VbSeqNoEntry struct {
	VbID  uint16
	SeqNo uint64
}

type GetVbucketSeqNosResponse struct {
	Entries []VbSeqNoEntry
}

func (o OpsCtrl) GetVbucketSeqNos(
	d Dispatcher,
	req *GetVbucketSeqNosRequest,
	cb func(*GetVbucketSeqNosResponse, error),
) (PendingOp, error) {
	var extraBuf []byte

	if req.CollectionID == 0 {
		extraBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(extraBuf[0:], uint32(req.VbucketState))
	} else {
		if !o.CollectionsEnabled {
			return nil, protocolError{"cannot use collection filter when collections not enabled"}
		}

		extraBuf = make([]byte, 8)
		binary.BigEndian.PutUint32(extraBuf[0:], uint32(req.VbucketState))
		binary.BigEndian.PutUint32(extraBuf[4:], req.CollectionID)
	}

	return d.Dispatch(&Packet{
		OpCode: OpCodeGetAllVBSeqnos,
		Extras: extraBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		decompErr := OpsCore{}.maybeDecompressPacket(resp)
		if decompErr != nil {
			cb(nil, decompErr)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		numEntries := len(resp.Value) / 10
		entries := make([]VbSeqNoEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = VbSeqNoEntry{
				VbID:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: binary.BigEndian.Uint64(resp.Value[i*10+2:]),
			}
		}

		cb(&GetVbucketSeqNosResponse{
			Entries: entries,
		}, nil)
		return false
	})
}
