package memdx

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"time"
)

type RangeScanCreateRequest struct {
	CollectionID uint32
	VbucketID    uint16
	Value        []byte

	OnBehalfOf string
}

type RangeScanCreateResponse struct {
	ScanUUUID []byte
}

func (o OpsCrud) RangeScanCreate(d Dispatcher, req *RangeScanCreateRequest, cb func(*RangeScanCreateResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	var value []byte
	if req.CollectionID > 0 {
		var reqValue map[string]json.RawMessage
		if err := json.Unmarshal(req.Value, &reqValue); err != nil {
			return nil, err
		}

		reqValue["collection"] = []byte(strconv.FormatUint(uint64(req.CollectionID), 16))
		value, err = json.Marshal(reqValue)
		if err != nil {
			return nil, err
		}
	} else {
		value = make([]byte, len(req.Value))
		copy(value, req.Value)
	}

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeRangeScanCreate,
		Datatype:      uint8(DatatypeFlagJSON),
		VbucketID:     req.VbucketID,
		FramingExtras: extFramesBuf,
		Value:         value,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrRangeEmpty)
			return false
		} else if resp.Status == StatusNotStored {
			cb(nil, ErrSeqNoNotFound)
			return false
		} else if resp.Status == StatusRangeError {
			cb(nil, ErrSampleRangeImpossible)
			return false
		} else if resp.Status == StatusRangeScanVbUUIDNotEqual {
			cb(nil, ErrVbUUIDMismatch)
			return false
		} else if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&RangeScanCreateResponse{
			ScanUUUID: resp.Value,
		}, nil)
		return false
	})
}

type RangeScanContinueRequest struct {
	ScanUUID  []byte
	MaxCount  uint32
	MaxBytes  uint32
	VbucketID uint16
	Deadline  time.Time

	OnBehalfOf string
}

type RangeScanContinueResponse struct {
	Value    []byte
	KeysOnly bool
	More     bool
	Complete bool
}

func (o OpsCrud) RangeScanContinue(d Dispatcher, req *RangeScanContinueRequest, cb func(*RangeScanContinueResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if len(req.ScanUUID) != 16 {
		return nil, invalidArgError{"scanUUID must be 16 bytes, was " + strconv.Itoa(len(req.ScanUUID))}
	}

	var deadlineMs uint32
	if !req.Deadline.IsZero() {
		deadlineMs = uint32(time.Until(req.Deadline).Milliseconds())
	}

	extraBuf := make([]byte, 28)
	copy(extraBuf[:16], req.ScanUUID)
	binary.BigEndian.PutUint32(extraBuf[16:], req.MaxCount)
	binary.BigEndian.PutUint32(extraBuf[20:], deadlineMs)
	binary.BigEndian.PutUint32(extraBuf[24:], req.MaxBytes)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeRangeScanContinue,
		VbucketID:     req.VbucketID,
		Extras:        extraBuf,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrScanNotFound)
			return false
		} else if resp.Status == StatusRangeScanCancelled {
			cb(nil, ErrRangeScanCancelled)
			return false
		} else if resp.Status != StatusSuccess && resp.Status != StatusRangeScanMore &&
			resp.Status != StatusRangeScanComplete {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		if len(resp.Extras) != 4 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		keysOnlyFlag := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&RangeScanContinueResponse{
			KeysOnly: keysOnlyFlag == 0,
			More:     resp.Status == StatusRangeScanMore,
			Complete: resp.Status == StatusRangeScanComplete,
			Value:    resp.Value,
		}, nil)

		// If range scan responds with status more then the caller must issue another request,
		// if the status is complete the scan is finished. Otherwise we can expect more
		// responses to this request.
		return resp.Status == StatusRangeScanMore || resp.Status == StatusRangeScanComplete
	})
}

type RangeScanCancelRequest struct {
	ScanUUID  []byte
	VbucketID uint16

	OnBehalfOf string
}

type RangeScanCancelResponse struct{}

func (o OpsCrud) RangeScanCancel(d Dispatcher, req *RangeScanCancelRequest, cb func(*RangeScanCancelResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if len(req.ScanUUID) != 16 {
		return nil, invalidArgError{"scanUUID must be 16 bytes, was " + strconv.Itoa(len(req.ScanUUID))}
	}

	extraBuf := make([]byte, 16)
	copy(extraBuf[:16], req.ScanUUID)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeRangeScanCancel,
		VbucketID:     req.VbucketID,
		Extras:        extraBuf,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrScanNotFound)
			return false
		} else if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&RangeScanCancelResponse{}, nil)
		return false
	})
}
