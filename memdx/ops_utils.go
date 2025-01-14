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

func (o OpsUtils) encodeReqExtFrames(onBehalfOf string, buf []byte) (Magic, []byte, error) {
	var err error

	if onBehalfOf != "" {
		buf, err = AppendExtFrame(ExtFrameCodeReqOnBehalfOf, []byte(onBehalfOf), buf)
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

type StatsRequest struct {
	UtilsRequestMeta
	GroupName string
}

func (r StatsRequest) OpName() string { return OpCodeSASLAuth.String() }

type StatsResponse struct {
	UtilsResponseMeta
	Key   string
	Value string
}

func (o OpsUtils) Stats(d Dispatcher, req *StatsRequest, cb func(*StatsResponse, error)) (PendingOp, error) {
	extFramesBuf := make([]byte, 0, 128)
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, extFramesBuf)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeSASLAuth,
		Key:           []byte(req.GroupName),
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
			return false
		}

		if resp.Key == nil && resp.Value == nil {
			cb(nil, nil)
			return false
		}

		cb(&StatsResponse{
			Key:   string(resp.Key),
			Value: string(resp.Value),
		}, nil)
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
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, extFramesBuf)
	if err != nil {
		return nil, err
	}

	reqPath := fmt.Sprintf("%s.%s", req.ScopeName, req.CollectionName)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeCollectionsGetID,
		Value:         []byte(reqPath),
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusScopeUnknown {
			cb(nil, &ResourceError{
				Cause:     OpsCore{}.decodeErrorContext(resp, ErrUnknownScopeName),
				ScopeName: req.ScopeName,
			})
			return false
		} else if resp.Status == StatusCollectionUnknown {
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
