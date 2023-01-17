package memdx

import (
	"encoding/binary"
)

type OpsCrud struct {
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
}

type GetResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) Get(d Dispatcher, req *GetRequest, cb func(*GetResponse, error)) error {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return err
	}

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGet,
		Key:    reqKey,
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
}

type GetAndTouchResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndTouch(d Dispatcher, req *GetAndTouchRequest, cb func(*GetAndTouchResponse, error)) error {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.Expiry)

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGAT,
		Key:    reqKey,
		Extras: extraBuf,
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
}

type GetAndLockResponse struct {
	Cas      uint64
	Flags    uint32
	Value    []byte
	Datatype uint8
}

func (o OpsCrud) GetAndLock(d Dispatcher, req *GetAndLockRequest, cb func(*GetAndLockResponse, error)) error {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return err
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], req.LockTime)

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetLocked,
		Key:    reqKey,
		Extras: extraBuf,
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

func (o OpsCrud) GetRandom(d Dispatcher, req *GetRandomRequest, cb func(*GetRandomResponse, error)) error {
	var extrasBuf []byte
	if !o.CollectionsEnabled {
		if req.CollectionID != 0 {
			return ErrCollectionsNotEnabled
		}

		// extrasBuf = nil
	} else {
		extrasBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(extrasBuf, req.CollectionID)
	}

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGet,
		Extras: extrasBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
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
