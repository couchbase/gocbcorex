package memdx

import (
	"encoding/binary"
	"fmt"
)

type OpsUtils struct {
	Dispatcher Dispatcher

	ExtFramesEnabled bool
}

func (o OpsUtils) encodeReqExtFrames(onBehalfOf string, buf []byte) (Magic, []byte, error) {
	var err error

	if !o.ExtFramesEnabled {
		return 0, nil, protocolError{"cannot use framing extras when its not enabled"}
	}

	if onBehalfOf != "" {
		buf, err = AppendExtFrame(ExtFrameCodeReqOnBehalfOf, []byte(onBehalfOf), buf)
		if err != nil {
			return 0, nil, err
		}
	}

	if len(buf) > 0 {
		return MagicReqExt, buf, nil
	}

	return MagicReq, buf, nil
}

type StatsRequest struct {
	GroupName string

	OnBehalfOf string
}

type StatsResponse struct {
	Key   string
	Value string
}

func (o OpsUtils) Stats(req *StatsRequest, cb func(*StatsResponse, error)) error {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	return o.Dispatcher.Dispatch(&Packet{
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
	ScopeName      string
	CollectionName string

	OnBehalfOf string
}

type GetCollectionIDResponse struct {
	ManifestID   uint64
	CollectionID uint32
}

func (o OpsUtils) GetCollectionID(req *GetCollectionIDRequest, cb func(*GetCollectionIDResponse, error)) error {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	reqPath := fmt.Sprintf("%s.%s", req.ScopeName, req.CollectionName)

	return o.Dispatcher.Dispatch(&Packet{
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
			cb(nil, ErrUnknownScopeName)
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionName)
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
			return false
		}

		if len(resp.Extras) != 12 {
			cb(nil, protocolError{"invalid extras length"})
			return false
		}

		manifestID := binary.BigEndian.Uint64(resp.Extras[0:])
		collectionID := binary.BigEndian.Uint32(resp.Extras[8:])

		cb(&GetCollectionIDResponse{
			ManifestID:   manifestID,
			CollectionID: collectionID,
		}, nil)
		return false
	})
}
