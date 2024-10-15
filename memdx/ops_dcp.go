package memdx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type OpsDcp struct {
	CollectionsEnabled bool
	ExtFramesEnabled   bool
	StreamIdsEnabled   bool
}

func (o OpsDcp) encodeReqExtFrames(
	streamId uint16,
	buf []byte,
) (Magic, []byte, error) {
	var err error

	if streamId > 0 {
		if !o.StreamIdsEnabled {
			return 0, nil, protocolError{"cannot use stream ids when its not enabled"}
		}

		streamIdBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(streamIdBuf, streamId)

		buf, err = AppendExtFrame(ExtFrameCodeReqStreamID, streamIdBuf, buf)
		if err != nil {
			return 0, nil, err
		}
	}

	if len(buf) > 0 {
		if !o.ExtFramesEnabled {
			return 0, nil, protocolError{"cannot use framing extras when its not enabled"}
		}

		return MagicReqExt, buf, nil
	}

	return MagicReq, nil, nil
}

type DcpOpenConnectionRequest struct {
	ConnectionName string
	ConsumerName   string
	Flags          DcpConnectionFlags
}

func (r DcpOpenConnectionRequest) OpName() string { return OpCodeDcpOpenConnection.String(MagicReq) }

type DcpOpenConnectionResponse struct {
}

type DcpOpenConnectionJson struct {
	ConsumerName string `json:"consumer_name"`
}

func (o OpsDcp) DcpOpenConnection(
	d Dispatcher,
	req *DcpOpenConnectionRequest,
	cb func(*DcpOpenConnectionResponse, error),
) (PendingOp, error) {
	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], uint32(req.Flags))

	var valueBuf []byte
	if req.ConsumerName != "" {
		valueObj := DcpOpenConnectionJson{
			ConsumerName: req.ConsumerName,
		}

		genValueBuf, err := json.Marshal(valueObj)
		if err != nil {
			return nil, err
		}

		valueBuf = genValueBuf
	}

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeDcpOpenConnection,
		Key:    []byte(req.ConnectionName),
		Value:  valueBuf,
		Extras: extraBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&DcpOpenConnectionResponse{}, nil)
		return false
	})
}

type DcpControlRequest struct {
	Key   string
	Value string
}

func (r DcpControlRequest) OpName() string { return OpCodeDcpControl.String(MagicReq) }

type DcpControlResponse struct {
}

func (o OpsDcp) DcpControl(d Dispatcher, req *DcpControlRequest, cb func(*DcpControlResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeDcpControl,
		Key:    []byte(req.Key),
		Value:  []byte(req.Value),
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&DcpControlResponse{}, nil)
		return false
	})
}

type DcpStreamReqRequest struct {
	VbucketID      uint16
	Flags          uint32
	StartSeqNo     uint64
	EndSeqNo       uint64
	VbUuid         uint64
	SnapStartSeqNo uint64
	SnapEndSeqNo   uint64

	ManifestUid   uint64
	StreamId      uint64
	ScopeId       uint64
	CollectionIds []uint64
}

func (r DcpStreamReqRequest) OpName() string { return OpCodeDcpStreamReq.String(MagicReq) }

type DcpStreamReqResponse struct {
	Opaque      uint32
	FailoverLog []DcpFailoverEntry
}

type DcpStreamReqJson struct {
	UID         string   `json:"uid,omitempty"`
	SID         string   `json:"sid,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	Collections []string `json:"collections,omitempty"`
}

func (o OpsDcp) DcpStreamReq(
	d Dispatcher, req *DcpStreamReqRequest,
	cb func(*DcpStreamReqResponse, error),
) (PendingOp, error) {
	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(req.Flags))
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(req.StartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(req.EndSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(req.VbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(req.SnapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(req.SnapEndSeqNo))

	var valueBuf []byte
	if req.StreamId > 0 || req.ManifestUid > 0 || req.ScopeId > 0 || len(req.CollectionIds) > 0 {
		if req.StreamId > 0 && !o.StreamIdsEnabled {
			return nil, protocolError{"cannot use stream ids when its not enabled"}
		}

		if (req.ManifestUid > 0 || req.ScopeId > 0 || len(req.CollectionIds) > 0) && !o.CollectionsEnabled {
			return nil, protocolError{"cannot use collection filters when collections not enabled"}
		}

		collectionHexIds := make([]string, len(req.CollectionIds))
		for colIdx, colId := range req.CollectionIds {
			collectionHexIds[colIdx] = fmt.Sprintf("%x", colId)
		}

		valueObj := DcpStreamReqJson{
			UID:         fmt.Sprintf("%x", req.ManifestUid),
			SID:         fmt.Sprintf("%x", req.StreamId),
			Scope:       fmt.Sprintf("%x", req.ScopeId),
			Collections: collectionHexIds,
		}

		genValueBuf, err := json.Marshal(valueObj)
		if err != nil {
			return nil, err
		}

		valueBuf = genValueBuf
	}

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeDcpStreamReq,
		Key:       nil,
		Value:     valueBuf,
		VbucketID: req.VbucketID,
		Extras:    extraBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusRollback {
			if len(resp.Value) != 8 {
				cb(nil, protocolError{"rollback error with bad value length"})
				return false
			}

			rollbackSeqNo := binary.BigEndian.Uint64(resp.Value[0:])

			cb(nil, &DcpRollbackError{
				RollbackSeqNo: rollbackSeqNo,
			})
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
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

		cb(&DcpStreamReqResponse{
			Opaque:      resp.Opaque,
			FailoverLog: entries,
		}, nil)
		return false
	})
}

type DcpCloseStreamRequest struct {
	VbucketID uint16
	StreamId  uint16
}

func (r DcpCloseStreamRequest) OpName() string { return OpCodeDcpCloseStream.String(MagicReq) }

type DcpCloseStreamResponse struct {
}

func (o OpsDcp) DcpCloseStream(
	d Dispatcher, req *DcpCloseStreamRequest,
	cb func(*DcpCloseStreamResponse, error),
) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.StreamId, nil)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:     reqMagic,
		OpCode:    OpCodeDcpCloseStream,
		Key:       nil,
		Value:     nil,
		VbucketID: req.VbucketID,
		Extras:    extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&DcpCloseStreamResponse{}, nil)
		return false
	})
}
