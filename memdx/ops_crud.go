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

type GetRequest struct {
	CollectionID uint32
	Key          []byte
	VbucketID    uint16
}

type GetResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) Get(d Dispatcher, req *GetRequest, cb func(*GetResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeGet,
		Key:       reqKey,
		VbucketID: req.VbucketID,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type GetAndTouchResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndTouch(d Dispatcher, req *GetAndTouchRequest, cb func(*GetAndTouchResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeGAT,
		Key:       reqKey,
		Extras:    extraBuf,
		VbucketID: req.VbucketID,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type GetAndLockResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndLock(d Dispatcher, req *GetAndLockRequest, cb func(*GetAndLockResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.LockTime)

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeGetLocked,
		Key:       reqKey,
		Extras:    extraBuf,
		VbucketID: req.VbucketID,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type GetRandomResponse struct {
	Key      []byte
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetRandom(d Dispatcher, req *GetRandomRequest, cb func(*GetRandomResponse, error)) (PendingOp, error) {
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
		Magic:  MagicReq,
		OpCode: OpCodeGetRandom,
		Extras: extrasBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type SetResponse struct {
	Cas uint64
}

func (o OpsCrud) Set(d Dispatcher, req *SetRequest, cb func(*SetResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Flags)
	binary.BigEndian.PutUint32(extraBuf[4:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeSet,
		Key:       reqKey,
		VbucketID: req.VbucketID,
		Datatype:  req.Datatype,
		Extras:    extraBuf,
		Value:     req.Value,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type UnlockResponse struct {
}

func (o OpsCrud) Unlock(d Dispatcher, req *UnlockRequest, cb func(*UnlockResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeUnlockKey,
		Key:       reqKey,
		VbucketID: req.VbucketID,
		Cas:       req.Cas,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
}

type TouchResponse struct {
	Cas uint64
}

func (o OpsCrud) Touch(d Dispatcher, req *TouchRequest, cb func(*TouchResponse, error)) (PendingOp, error) {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return nil, err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:     MagicReq,
		OpCode:    OpCodeTouch,
		Key:       reqKey,
		VbucketID: req.VbucketID,
		Extras:    extraBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(nil, ErrDocNotFound)
			return false
		} else if resp.Status == StatusCollectionUnknown {
			cb(nil, ErrUnknownCollectionID)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, OpsCore{}.decodeError(resp))
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
