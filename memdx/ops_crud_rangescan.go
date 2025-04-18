package memdx

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"time"
)

func (o OpsCrud) RangeScanCreate(d Dispatcher, req *RangeScanCreateRequest, cb func(*RangeScanCreateResponse, error)) (PendingOp, error) {
	extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, 0, 0, false, nil)
	if err != nil {
		return nil, err
	}

	value, err := req.toJSON()
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
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
			cb(nil, ErrRangeScanEmpty)
			return false
		} else if resp.Status == StatusNotStored {
			cb(nil, ErrRangeScanSeqNoNotFound)
			return false
		} else if resp.Status == StatusRangeError {
			cb(nil, ErrRangeScanRangeError)
			return false
		} else if resp.Status == StatusRangeScanVbUUIDNotEqual {
			cb(nil, ErrRangeScanVbUuidMismatch)
			return false
		} else if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		cb(&RangeScanCreateResponse{
			ScanUUUID: resp.Value,
		}, nil)
		return false
	})
}

func (o OpsCrud) RangeScanContinue(d Dispatcher, req *RangeScanContinueRequest, dataCb func(*RangeScanDataResponse) error,
	actionCb func(*RangeScanActionResponse, error)) (PendingOp, error) {
	extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, 0, 0, false, nil)
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
		OpCode:        OpCodeRangeScanContinue,
		VbucketID:     req.VbucketID,
		Extras:        extraBuf,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			actionCb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			actionCb(nil, ErrRangeScanNotFound)
			return false
		} else if resp.Status == StatusRangeScanCancelled {
			actionCb(nil, ErrRangeScanCancelled)
			return false
		} else if resp.Status != StatusSuccess && resp.Status != StatusRangeScanMore &&
			resp.Status != StatusRangeScanComplete {
			actionCb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) != 4 {
			actionCb(nil, protocolError{"bad extras length"})
			return false
		}

		includesContentFlag := binary.BigEndian.Uint32(resp.Extras[0:])
		items := o.parseRangeScanData(resp.Value, includesContentFlag == 0)

		if len(items) > 0 {
			err := dataCb(&RangeScanDataResponse{
				KeysOnly: includesContentFlag == 0,
				Items:    items,
			})
			if err != nil {
				// if an error is returned from the dataCb, we should stop the range scan and
				// return that error back to the caller in the final callback.
				actionCb(nil, err)
				return false
			}
		}

		if resp.Status == StatusRangeScanMore ||
			resp.Status == StatusRangeScanComplete {
			actionCb(&RangeScanActionResponse{
				More:     resp.Status == StatusRangeScanMore,
				Complete: resp.Status == StatusRangeScanComplete,
			}, nil)

			// If range scan responds with status more then the caller must issue another request,
			// if the status is complete the scan is finished.
			return false
		}

		// This means that the status is success, which indicates that there are more packets to comes.
		return true
	})
}

func (o OpsCrud) RangeScanCancel(d Dispatcher, req *RangeScanCancelRequest, cb func(*RangeScanCancelResponse, error)) (PendingOp, error) {
	extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, 0, 0, false, nil)
	if err != nil {
		return nil, err
	}

	if len(req.ScanUUID) != 16 {
		return nil, invalidArgError{"scanUUID must be 16 bytes, was " + strconv.Itoa(len(req.ScanUUID))}
	}

	extraBuf := make([]byte, 16)
	copy(extraBuf[:16], req.ScanUUID)

	return d.Dispatch(&Packet{
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
			cb(nil, ErrRangeScanNotFound)
			return false
		} else if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		cb(&RangeScanCancelResponse{}, nil)
		return false
	})
}

// RangeScanCreateRangeScanConfig is the configuration available for performing a range scan.
type RangeScanCreateRangeScanConfig struct {
	Start          []byte
	End            []byte
	ExclusiveStart []byte
	ExclusiveEnd   []byte
}

func (cfg *RangeScanCreateRangeScanConfig) hasStart() bool {
	return len(cfg.Start) > 0
}

func (cfg *RangeScanCreateRangeScanConfig) hasEnd() bool {
	return len(cfg.End) > 0
}
func (cfg *RangeScanCreateRangeScanConfig) hasExclusiveStart() bool {
	return len(cfg.ExclusiveStart) > 0
}

func (cfg *RangeScanCreateRangeScanConfig) hasExclusiveEnd() bool {
	return len(cfg.ExclusiveEnd) > 0
}

// RangeScanCreateRandomSamplingConfig is the configuration available for performing a random sampling.
type RangeScanCreateRandomSamplingConfig struct {
	Seed    uint64
	Samples uint64
}

// RangeScanCreateSnapshotRequirements is the set of requirements that the vbucket snapshot must meet in-order for
// the request to be successful.
type RangeScanCreateSnapshotRequirements struct {
	VbUUID      uint64
	SeqNo       uint64
	SeqNoExists bool
	Deadline    time.Time
}

type RangeScanCreateRequest struct {
	CrudRequestMeta

	CollectionID uint32
	VbucketID    uint16

	Scan     json.RawMessage
	KeysOnly bool
	Range    *RangeScanCreateRangeScanConfig
	Sampling *RangeScanCreateRandomSamplingConfig
	Snapshot *RangeScanCreateSnapshotRequirements
}

func (r RangeScanCreateRequest) OpName() string { return OpCodeRangeScanCreate.String() }

func (opts RangeScanCreateRequest) toJSON() ([]byte, error) {
	if opts.Range != nil && opts.Sampling != nil {
		return nil, invalidArgError{"only one of range and sampling can be set"}
	}
	if opts.Range == nil && opts.Sampling == nil {
		return nil, invalidArgError{"one of range and sampling must set"}
	}

	var collection string
	if opts.CollectionID != 0 {
		collection = strconv.FormatUint(uint64(opts.CollectionID), 16)
	}
	createReq := &rangeScanCreateRequestJSON{
		Collection: collection,
		KeyOnly:    opts.KeysOnly,
	}

	if opts.Range != nil {
		if opts.Range.hasStart() && opts.Range.hasExclusiveStart() {
			return nil, invalidArgError{"only one of start and exclusive start within range can be set"}
		}
		if opts.Range.hasEnd() && opts.Range.hasExclusiveEnd() {
			return nil, invalidArgError{"only one of end and exclusive end within range can be set"}
		}
		if !(opts.Range.hasStart() || opts.Range.hasExclusiveStart()) {
			return nil, invalidArgError{"one of start and exclusive start within range must both be set"}
		}
		if !(opts.Range.hasEnd() || opts.Range.hasExclusiveEnd()) {
			return nil, invalidArgError{"one of end and exclusive end within range must both be set"}
		}

		createReq.Range = &rangeScanCreateRangeJSON{}
		if len(opts.Range.Start) > 0 {
			createReq.Range.Start = base64.StdEncoding.EncodeToString(opts.Range.Start)
		}
		if len(opts.Range.End) > 0 {
			createReq.Range.End = base64.StdEncoding.EncodeToString(opts.Range.End)
		}
		if len(opts.Range.ExclusiveStart) > 0 {
			createReq.Range.ExclusiveStart = base64.StdEncoding.EncodeToString(opts.Range.ExclusiveStart)
		}
		if len(opts.Range.ExclusiveEnd) > 0 {
			createReq.Range.ExclusiveEnd = base64.StdEncoding.EncodeToString(opts.Range.ExclusiveEnd)
		}
	}

	if opts.Sampling != nil {
		if opts.Sampling.Samples == 0 {
			return nil, invalidArgError{"samples within sampling must be set"}
		}

		createReq.Sampling = &rangeScanCreateSampleJSON{
			Seed:    opts.Sampling.Seed,
			Samples: opts.Sampling.Samples,
		}
	}

	if opts.Snapshot != nil {
		if opts.Snapshot.VbUUID == 0 {
			return nil, invalidArgError{"vbuuid within snapshot must be set"}
		}
		if opts.Snapshot.SeqNo == 0 {
			return nil, invalidArgError{"seqno within snapshot must be set"}
		}

		createReq.Snapshot = &rangeScanCreateSnapshotJSON{
			VbUUID:      strconv.FormatUint(opts.Snapshot.VbUUID, 10),
			SeqNo:       opts.Snapshot.SeqNo,
			SeqNoExists: opts.Snapshot.SeqNoExists,
		}
		createReq.Snapshot.Timeout = uint64(time.Until(opts.Snapshot.Deadline) / time.Millisecond)
	}

	return json.Marshal(createReq)
}

type RangeScanCreateResponse struct {
	ScanUUUID []byte
}

// RangeScanItem encapsulates an iterm returned during a range scan.
type RangeScanItem struct {
	Value    []byte
	Key      []byte
	Flags    uint32
	Cas      uint64
	Expiry   uint32
	SeqNo    uint64
	Datatype uint8
}

type RangeScanContinueRequest struct {
	CrudRequestMeta

	ScanUUID  []byte
	MaxCount  uint32
	MaxBytes  uint32
	VbucketID uint16
	Deadline  time.Time
}

func (r RangeScanContinueRequest) OpName() string { return OpCodeRangeScanContinue.String() }

type RangeScanDataResponse struct {
	Items    []RangeScanItem
	KeysOnly bool
}

type RangeScanActionResponse struct {
	More     bool
	Complete bool
}

type RangeScanCancelRequest struct {
	CrudRequestMeta

	ScanUUID  []byte
	VbucketID uint16
}

func (r RangeScanCancelRequest) OpName() string { return OpCodeRangeScanCancel.String() }

type RangeScanCancelResponse struct{}

type rangeScanCreateRequestJSON struct {
	Collection string                       `json:"collection,omitempty"`
	KeyOnly    bool                         `json:"key_only,omitempty"`
	Range      *rangeScanCreateRangeJSON    `json:"range,omitempty"`
	Sampling   *rangeScanCreateSampleJSON   `json:"sampling,omitempty"`
	Snapshot   *rangeScanCreateSnapshotJSON `json:"snapshot_requirements,omitempty"`
}

type rangeScanCreateRangeJSON struct {
	Start          string `json:"start,omitempty"`
	End            string `json:"end,omitempty"`
	ExclusiveStart string `json:"excl_start,omitempty"`
	ExclusiveEnd   string `json:"excl_end,omitempty"`
}

type rangeScanCreateSampleJSON struct {
	Seed    uint64 `json:"seed,omitempty"`
	Samples uint64 `json:"samples"`
}

type rangeScanCreateSnapshotJSON struct {
	VbUUID      string `json:"vb_uuid"`
	SeqNo       uint64 `json:"seqno"`
	SeqNoExists bool   `json:"seqno_exists,omitempty"`
	Timeout     uint64 `json:"timeout_ms,omitempty"`
}

func (o OpsCrud) parseRangeScanData(data []byte, keysOnly bool) []RangeScanItem {
	if keysOnly {
		return o.parseRangeScanKeys(data)
	}

	return o.parseRangeScanDocs(data)
}

func (o OpsCrud) parseRangeScanLebEncoded(data []byte) ([]byte, uint64) {
	keyLen, n := binary.Uvarint(data)
	keyLen = keyLen + uint64(n)
	return data[uint64(n):keyLen], keyLen
}

func (o OpsCrud) parseRangeScanKeys(data []byte) []RangeScanItem {
	var keys []RangeScanItem
	var i uint64
	dataLen := uint64(len(data))
	for {
		if i >= dataLen {
			break
		}

		key, n := o.parseRangeScanLebEncoded(data[i:])
		keys = append(keys, RangeScanItem{
			Key: key,
		})
		i = i + n
	}

	return keys
}

func (o OpsCrud) parseRangeScanItem(data []byte) (RangeScanItem, uint64) {
	flags := binary.BigEndian.Uint32(data[0:])
	expiry := binary.BigEndian.Uint32(data[4:])
	seqno := binary.BigEndian.Uint64(data[8:])
	cas := binary.BigEndian.Uint64(data[16:])
	datatype := data[24]
	key, n := o.parseRangeScanLebEncoded(data[25:])
	value, n2 := o.parseRangeScanLebEncoded(data[25+n:])

	return RangeScanItem{
		Value:    value,
		Key:      key,
		Flags:    flags,
		Cas:      cas,
		Expiry:   expiry,
		SeqNo:    seqno,
		Datatype: datatype,
	}, 25 + n + n2
}

func (o OpsCrud) parseRangeScanDocs(data []byte) []RangeScanItem {
	var items []RangeScanItem
	var i uint64
	dataLen := uint64(len(data))
	for {
		if i >= dataLen {
			break
		}

		item, n := o.parseRangeScanItem(data[i:])
		items = append(items, item)
		i = i + n
	}

	return items
}
