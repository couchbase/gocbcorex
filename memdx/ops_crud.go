package memdx

import (
	"encoding/binary"
)

type OpsCrud struct {
	Dispatcher         Dispatcher
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

func (o OpsCrud) Get(req *GetRequest, cb func(*GetResponse, error)) error {
	reqKey, err := o.encodeCollectionAndKey(req.CollectionID, req.Key, nil)
	if err != nil {
		return err
	}

	return o.Dispatcher.Dispatch(&Packet{
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

func (o OpsCrud) GetRandom(req *GetRandomRequest, cb func(*GetRandomResponse, error)) error {
	var extrasBuf []byte
	if !o.CollectionsEnabled {
		if req.CollectionID != 0 {
			return ErrCollectionsNotEnabled
		}

		// extrasBuf = nil
	} else {
		extrasBuf = binary.BigEndian.AppendUint32(extrasBuf, req.CollectionID)
	}

	return o.Dispatcher.Dispatch(&Packet{
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
