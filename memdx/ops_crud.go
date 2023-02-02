package memdx

import (
	"encoding/binary"
)

type OpsCrud struct {
	ExtFramesEnabled   bool
	CollectionsEnabled bool
}

func (o OpsCrud) encodeCollectionAndKey(collectionID uint32, key []byte, buf []byte) ([]byte, error) {
	if !o.CollectionsEnabled {
		if collectionID != 0 {
			return nil, ErrCollectionsNotEnabled
		}

		// we intentionally copy to the buffer here so that key does not escape
		buf = append(buf, key...)
		return buf, nil
	}

	return AppendCollectionIDAndKey(collectionID, key, buf)
}

// TODO(brett19): This exists in OpsUtils too, we should probably deduplicate the implementation.
func (o OpsCrud) encodeReqExtFrames(onBehalfOf string, buf []byte) (Magic, []byte, error) {
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

func (o OpsCrud) decodeCommonError(resp *Packet) error {
	switch resp.Status {
	case StatusCollectionUnknown:
		return ErrUnknownCollectionID
	default:
		return OpsCore{}.decodeError(resp)
	}
}

type GetRequest struct {
	CollectionID uint32
	Key          []byte
	VbucketID    uint16

	OnBehalfOf string
}

type GetResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) Get(d Dispatcher, req *GetRequest, cb func(*GetResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeGet,
		Key:           reqKey,
		VbucketID:     req.VbucketID,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) != 4 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetResponse{
			Cas:      resp.Cas,
			Flags:    flags,
			Value:    resp.Value,
			Datatype: resp.Datatype,
		}, nil)
		return false
	})
}

type GetAndTouchRequest struct {
	CollectionID uint32
	Expiry       uint32
	Key          []byte
	VbucketID    uint16
	OnBehalfOf   string
}

type GetAndTouchResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndTouch(d Dispatcher, req *GetAndTouchRequest, cb func(*GetAndTouchResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeGAT,
		Key:           reqKey,
		Extras:        extraBuf,
		VbucketID:     req.VbucketID,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusKeyExists {
			cb(nil, ErrDocLocked)
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) != 4 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetAndTouchResponse{
			Cas:      resp.Cas,
			Flags:    flags,
			Value:    resp.Value,
			Datatype: resp.Datatype,
		}, nil)
		return false
	})
}

type GetAndLockRequest struct {
	CollectionID uint32
	LockTime     uint32
	Key          []byte
	VbucketID    uint16

	OnBehalfOf string
}

type GetAndLockResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndLock(d Dispatcher, req *GetAndLockRequest, cb func(*GetAndLockResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.LockTime)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeGetLocked,
		Key:           reqKey,
		Extras:        extraBuf,
		VbucketID:     req.VbucketID,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusKeyExists {
			cb(nil, ErrDocLocked)
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) != 4 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetAndLockResponse{
			Cas:      resp.Cas,
			Flags:    flags,
			Value:    resp.Value,
			Datatype: resp.Datatype,
		}, nil)
		return false
	})
}

type GetRandomRequest struct {
	CollectionID uint32

	OnBehalfOf string
}

type GetRandomResponse struct {
	Key      []byte
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetRandom(d Dispatcher, req *GetRandomRequest, cb func(*GetRandomResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	var extrasBuf []byte
	if o.CollectionsEnabled {
		extrasBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf, req.CollectionID)
	} else {
		if req.CollectionID != 0 {
			return nil, ErrCollectionsNotEnabled
		}

		// extrasBuf = nil
	}

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeGetRandom,
		Extras:        extrasBuf,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) != 4 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		flags := binary.BigEndian.Uint32(resp.Extras[0:])

		cb(&GetRandomResponse{
			Key:      resp.Key,
			Cas:      resp.Cas,
			Flags:    flags,
			Value:    resp.Value,
			Datatype: resp.Datatype,
		}, nil)
		return false
	})
}

type SetRequest struct {
	CollectionID uint32
	Key          []byte
	VbucketID    uint16
	Flags        uint32
	Value        []byte
	Datatype     uint8
	Expiry       uint32
	OnBehalfOf   string
}

type SetResponse struct {
	Cas uint64
}

func (o OpsCrud) Set(d Dispatcher, req *SetRequest, cb func(*SetResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeSet,
		Key:           reqKey,
		VbucketID:     req.VbucketID,
		Datatype:      req.Datatype,
		Extras:        extraBuf,
		Value:         req.Value,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) == 16 {
			// parse mutation token
		} else if len(resp.Extras) != 0 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		cb(&SetResponse{
			Cas: resp.Cas,
		}, nil)
		return false
	})
}

type UnlockRequest struct {
	CollectionID uint32
	Cas          uint64
	Key          []byte
	VbucketID    uint16

	OnBehalfOf string
}

type UnlockResponse struct {
}

func (o OpsCrud) Unlock(d Dispatcher, req *UnlockRequest, cb func(*UnlockResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeUnlockKey,
		Key:           reqKey,
		VbucketID:     req.VbucketID,
		Cas:           req.Cas,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusKeyExists {
			cb(nil, ErrCasMismatch)
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) == 16 {
			// parse mutation token
		} else if len(resp.Extras) != 0 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		cb(&UnlockResponse{}, nil)
		return false
	})
}

type TouchRequest struct {
	CollectionID uint32
	Key          []byte
	VbucketID    uint16
	Expiry       uint32
	OnBehalfOf   string
}

type TouchResponse struct {
	Cas uint64
}

func (o OpsCrud) Touch(d Dispatcher, req *TouchRequest, cb func(*TouchResponse, error)) (PendingOp, error) {
	reqMagic, extFramesBuf, err := o.encodeReqExtFrames(req.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:         reqMagic,
		OpCode:        OpCodeTouch,
		Key:           reqKey,
		VbucketID:     req.VbucketID,
		Extras:        extraBuf,
		FramingExtras: extFramesBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusKeyExists {
			cb(nil, ErrDocLocked)
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCrud{}.decodeCommonError(resp))
			return false
		}

		if len(resp.Extras) == 16 {
			// parse mutation token
		} else if len(resp.Extras) != 0 {
			cb(nil, protocolError{"bad extras length"})
			return false
		}

		cb(&TouchResponse{
			Cas: resp.Cas,
		}, nil)
		return false
	})
}
