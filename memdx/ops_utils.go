package memdx

import (
	"encoding/binary"
	"fmt"
)

type UtilsRequestMeta struct {
	OnBehalfOf string
}

type UtilsResponseMeta struct {
	// While these operations typically support ServerDuration, they wouldn't make
	// a lot of sense to expose here, so we do not.
}

type OpsUtils struct {
	ExtFramesEnabled bool
}

func (o OpsUtils) encodeReqExtFrames(onBehalfOf string, buf []byte) ([]byte, error) {
	var err error

	if onBehalfOf != "" {
		buf, err = AppendExtFrame(ExtFrameCodeReqOnBehalfOf, []byte(onBehalfOf), buf)
		if err != nil {
			return nil, err
		}
	}

	if len(buf) > 0 {
		if !o.ExtFramesEnabled {
			return nil, protocolError{"cannot use framing extras when its not enabled"}
		}

		return buf, nil
	}

	return nil, nil
}

type StatsRequest struct {
	UtilsRequestMeta
	GroupName string
}

func (r StatsRequest) OpName() string { return OpCodeSASLAuth.String() }

type StatsDataResponse struct {
	Key   string
	Value string
}

type StatsActionResponse struct {
	UtilsResponseMeta
}

func (o OpsUtils) Stats(
	d Dispatcher,
	req *StatsRequest,
	dataCb func(*StatsDataResponse),
	actionCb func(*StatsActionResponse, error),
) (PendingOp, error) {
	extFramesBuf := make([]byte, 0, 128)
	extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, extFramesBuf)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		OpCode:        OpCodeStat,
		Key:           []byte(req.GroupName),
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			actionCb(nil, err)
			return false
		}

		decompErr := OpsCore{}.maybeDecompressPacket(resp)
		if decompErr != nil {
			actionCb(nil, decompErr)
			return false
		}

		if resp.Status != StatusSuccess {
			actionCb(nil, OpsCore{}.decodeError(resp))
			return false
		}

		if len(resp.Key) == 0 && len(resp.Value) == 0 {
			actionCb(&StatsActionResponse{}, nil)
			return false
		}

		dataCb(&StatsDataResponse{
			Key:   string(resp.Key),
			Value: string(resp.Value),
		})
		return true
	})
}

type GetCollectionIDRequest struct {
	UtilsRequestMeta
	ScopeName      string
	CollectionName string
}

func (r GetCollectionIDRequest) OpName() string { return OpCodeCollectionsGetID.String() }

type GetCollectionIDResponse struct {
	UtilsResponseMeta
	ManifestRev  uint64
	CollectionID uint32
}

func (o OpsUtils) GetCollectionID(d Dispatcher, req *GetCollectionIDRequest, cb func(*GetCollectionIDResponse, error)) (PendingOp, error) {
	extFramesBuf := make([]byte, 0, 128)
	extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, extFramesBuf)
	if err != nil {
		return nil, err
	}

	reqPath := fmt.Sprintf("%s.%s", req.ScopeName, req.CollectionName)

	return d.Dispatch(&Packet{
		OpCode:        OpCodeCollectionsGetID,
		Value:         []byte(reqPath),
		FramingExtras: extFramesBuf,
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

		switch resp.Status {
		case StatusScopeUnknown:
			cb(nil, &ResourceError{
				Cause:     OpsCore{}.decodeErrorContext(resp, ErrUnknownScopeName),
				ScopeName: req.ScopeName,
			})
			return false
		case StatusCollectionUnknown:
			cb(nil, &ResourceError{
				Cause:          OpsCore{}.decodeErrorContext(resp, ErrUnknownCollectionName),
				ScopeName:      req.ScopeName,
				CollectionName: req.CollectionName,
			})
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
			return false
		}

		if len(resp.Extras) != 12 {
			cb(nil, protocolError{"invalid extras length"})
			return false
		}

		manifestRev := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

		cb(&GetCollectionIDResponse{
			ManifestRev:  manifestRev,
			CollectionID: collectionID,
		}, nil)
		return false
	})
}
