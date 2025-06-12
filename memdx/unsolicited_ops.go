package memdx

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Replier interface {
	WritePacket(*Packet) error
}

type UnsolicitedOpsHandlers struct {
	ClustermapChange func(req *ClustermapChangeEvent) error

	DcpSnapshotMarker     func(req *DcpSnapshotMarkerEvent) error
	DcpMutation           func(req *DcpMutationEvent) error
	DcpDeletion           func(req *DcpDeletionEvent) error
	DcpExpiration         func(req *DcpExpirationEvent) error
	DcpCollectionCreation func(req *DcpCollectionCreationEvent) error
	DcpCollectionDeletion func(req *DcpCollectionDeletionEvent) error
	DcpCollectionFlush    func(req *DcpCollectionFlushEvent) error
	DcpScopeCreation      func(req *DcpScopeCreationEvent) error
	DcpScopeDeletion      func(req *DcpScopeDeletionEvent) error
	DcpCollectionChanged  func(req *DcpCollectionModificationEvent) error
	DcpStreamEnd          func(req *DcpStreamEndEvent) error
	DcpOSOSnapshot        func(req *DcpOSOSnapshotEvent) error
	DcpSeqNoAdvanced      func(req *DcpSeqNoAdvancedEvent) error
	DcpNoOp               func(req *DcpNoOpEvent) (*DcpNoOpEventResponse, error)
}

type UnsolicitedOpsParser struct {
	CollectionsEnabled bool
}

func (o UnsolicitedOpsParser) decodeExtFrames(
	buf []byte,
) (uint16, error) {
	var streamId uint16
	err := IterExtFrames(buf, func(code ExtFrameCode, data []byte) {
		if code == ExtFrameCodeReqStreamID {
			streamId = binary.BigEndian.Uint16(data[0:])
		}
	})
	if err != nil {
		return 0, err
	}

	return streamId, nil
}

func (o UnsolicitedOpsParser) decodeCollectionAndKey(key []byte) (uint32, []byte, error) {
	if !o.CollectionsEnabled {
		return 0, key, nil
	}

	return DecodeCollectionIDAndKey(key)
}

func (o UnsolicitedOpsParser) parseDcpSnapshotMarker(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpSnapshotMarker == nil {
		return errors.New("unhandled DcpSnapshotMarker event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 20 {
		// Length of 20 indicates a v1 packet
		return handlers.DcpSnapshotMarker(&DcpSnapshotMarkerEvent{
			StreamId:           streamId,
			Version:            1,
			StartSeqNo:         binary.BigEndian.Uint64(pak.Extras[0:]),
			EndSeqNo:           binary.BigEndian.Uint64(pak.Extras[8:]),
			SnapshotType:       DcpSnapshotState(binary.BigEndian.Uint32(pak.Extras[16:])),
			MaxVisibleSeqNo:    0,
			HighCompletedSeqNo: 0,
			SnapshotTimeStamp:  0,
		})
	} else if len(pak.Extras) == 1 {
		// Length of 1 indicates v2+ packet
		version := pak.Extras[0]
		switch version {
		case 0:
			return handlers.DcpSnapshotMarker(&DcpSnapshotMarkerEvent{
				StreamId:           streamId,
				Version:            2,
				StartSeqNo:         binary.BigEndian.Uint64(pak.Extras[0:]),
				EndSeqNo:           binary.BigEndian.Uint64(pak.Extras[8:]),
				SnapshotType:       DcpSnapshotState(binary.BigEndian.Uint32(pak.Extras[16:])),
				MaxVisibleSeqNo:    binary.BigEndian.Uint64(pak.Value[20:]),
				HighCompletedSeqNo: binary.BigEndian.Uint64(pak.Value[28:]),
				SnapshotTimeStamp:  0,
			})
		case 1:
			return handlers.DcpSnapshotMarker(&DcpSnapshotMarkerEvent{
				StreamId:           streamId,
				Version:            3,
				StartSeqNo:         binary.BigEndian.Uint64(pak.Extras[0:]),
				EndSeqNo:           binary.BigEndian.Uint64(pak.Extras[8:]),
				SnapshotType:       DcpSnapshotState(binary.BigEndian.Uint32(pak.Extras[16:])),
				MaxVisibleSeqNo:    binary.BigEndian.Uint64(pak.Value[20:]),
				HighCompletedSeqNo: binary.BigEndian.Uint64(pak.Value[28:]),
				SnapshotTimeStamp:  binary.BigEndian.Uint64(pak.Value[36:]),
			})
		default:
			return &protocolError{"unknown dcp snapshot marker version"}
		}
	} else {
		return &protocolError{"unknown dcp snapshot marker format"}
	}
}

func (o UnsolicitedOpsParser) parseDcpMutation(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpMutation == nil {
		return errors.New("unhandled DcpMutation event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	collectionId, key, err := o.decodeCollectionAndKey(pak.Key)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 31 {
		return handlers.DcpMutation(&DcpMutationEvent{
			StreamId:     streamId,
			Version:      1,
			SeqNo:        binary.BigEndian.Uint64(pak.Extras[0:]),
			RevNo:        binary.BigEndian.Uint64(pak.Extras[8:]),
			Flags:        binary.BigEndian.Uint32(pak.Extras[16:]),
			Expiry:       binary.BigEndian.Uint32(pak.Extras[20:]),
			LockTime:     binary.BigEndian.Uint32(pak.Extras[24:]),
			MetaDataSize: binary.BigEndian.Uint16(pak.Extras[28:]),
			NRU:          pak.Extras[30],
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			VbucketId:    pak.VbucketID,
			CollectionId: collectionId,
			Key:          key,
			Value:        pak.Value,
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpDeletion(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpDeletion == nil {
		return errors.New("unhandled DcpDeletion event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	collectionId, key, err := o.decodeCollectionAndKey(pak.Key)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 18 {
		return handlers.DcpDeletion(&DcpDeletionEvent{
			StreamId:     streamId,
			Version:      1,
			SeqNo:        binary.BigEndian.Uint64(pak.Extras[0:]),
			RevNo:        binary.BigEndian.Uint64(pak.Extras[8:]),
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			VbucketId:    pak.VbucketID,
			CollectionId: collectionId,
			Key:          key,
			DeleteTime:   0,
		})
	} else if len(pak.Extras) == 21 {
		return handlers.DcpDeletion(&DcpDeletionEvent{
			StreamId:     streamId,
			Version:      2,
			SeqNo:        binary.BigEndian.Uint64(pak.Extras[0:]),
			RevNo:        binary.BigEndian.Uint64(pak.Extras[8:]),
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			VbucketId:    pak.VbucketID,
			CollectionId: collectionId,
			Key:          key,
			DeleteTime:   binary.BigEndian.Uint32(pak.Extras[16:]),
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpExpiration(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpExpiration == nil {
		return errors.New("unhandled DcpExpiration event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	collectionId, key, err := o.decodeCollectionAndKey(pak.Key)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 20 {
		return handlers.DcpExpiration(&DcpExpirationEvent{
			StreamId:     streamId,
			Version:      2,
			SeqNo:        binary.BigEndian.Uint64(pak.Extras[0:]),
			RevNo:        binary.BigEndian.Uint64(pak.Extras[8:]),
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			VbucketId:    pak.VbucketID,
			CollectionId: collectionId,
			Key:          key,
			DeleteTime:   binary.BigEndian.Uint32(pak.Extras[16:]),
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpEvent(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 13 {
		seqNo := binary.BigEndian.Uint64(pak.Extras[0:])
		eventCode := DcpStreamEventCode(binary.BigEndian.Uint32(pak.Extras[8:]))
		version := pak.Extras[12]

		switch eventCode {
		case DcpStreamEventCodeCollectionCreate:
			if handlers.DcpCollectionCreation == nil {
				return errors.New("unhandled DcpCollectionCreation event")
			}

			switch version {
			case 0:
				return handlers.DcpCollectionCreation(&DcpCollectionCreationEvent{
					StreamId:       streamId,
					Version:        0,
					SeqNo:          seqNo,
					VbucketId:      pak.VbucketID,
					ManifestUid:    binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeId:        binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionId:   binary.BigEndian.Uint32(pak.Value[12:]),
					CollectionName: string(pak.Key),
					Ttl:            0,
				})
			case 1:
				return handlers.DcpCollectionCreation(&DcpCollectionCreationEvent{
					StreamId:       streamId,
					Version:        1,
					SeqNo:          seqNo,
					VbucketId:      pak.VbucketID,
					ManifestUid:    binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeId:        binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionId:   binary.BigEndian.Uint32(pak.Value[12:]),
					CollectionName: string(pak.Key),
					Ttl:            binary.BigEndian.Uint32(pak.Value[16:]),
				})
			default:
				return &protocolError{"unknown dcp collection create event version"}
			}
		case DcpStreamEventCodeCollectionDelete:
			if handlers.DcpCollectionDeletion == nil {
				return errors.New("unhandled DcpCollectionDeletion event")
			}

			switch version {
			case 0:
				return handlers.DcpCollectionDeletion(&DcpCollectionDeletionEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbucketId:    pak.VbucketID,
					ManifestUid:  binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:      binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionId: binary.BigEndian.Uint32(pak.Value[12:]),
				})
			default:
				return &protocolError{"unknown dcp collection delete event version"}
			}
		case DcpStreamEventCodeCollectionFlush:
			if handlers.DcpCollectionFlush == nil {
				return errors.New("unhandled DcpCollectionFlush event")
			}

			switch version {
			case 0:
				return handlers.DcpCollectionFlush(&DcpCollectionFlushEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbucketId:    pak.VbucketID,
					ManifestUid:  binary.BigEndian.Uint64(pak.Value[0:]),
					CollectionId: binary.BigEndian.Uint32(pak.Value[8:]),
				})
			default:
				return &protocolError{"unknown dcp collection flush event version"}
			}
		case DcpStreamEventCodeScopeCreate:
			if handlers.DcpScopeCreation == nil {
				return errors.New("unhandled DcpScopeCreation event")
			}

			switch version {
			case 0:
				return handlers.DcpScopeCreation(&DcpScopeCreationEvent{
					StreamId:    streamId,
					Version:     0,
					SeqNo:       seqNo,
					VbucketId:   pak.VbucketID,
					ManifestUid: binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeId:     binary.BigEndian.Uint32(pak.Value[8:]),
					ScopeName:   string(pak.Key),
				})
			default:
				return &protocolError{"unknown dcp scope create event version"}
			}
		case DcpStreamEventCodeScopeDelete:
			if handlers.DcpScopeDeletion == nil {
				return errors.New("unhandled DcpScopeDeletion event")
			}

			switch version {
			case 0:
				return handlers.DcpScopeDeletion(&DcpScopeDeletionEvent{
					StreamId:    streamId,
					Version:     0,
					SeqNo:       seqNo,
					VbucketId:   pak.VbucketID,
					ManifestUid: binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeId:     binary.BigEndian.Uint32(pak.Value[8:]),
				})
			default:
				return &protocolError{"unknown dcp scope delete event version"}
			}
		case DcpStreamEventCodeCollectionChanged:
			if handlers.DcpCollectionChanged == nil {
				return errors.New("unhandled DcpCollectionChanged event")
			}

			switch version {
			case 0:
				return handlers.DcpCollectionChanged(&DcpCollectionModificationEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbucketId:    pak.VbucketID,
					ManifestUid:  binary.BigEndian.Uint64(pak.Value[0:]),
					CollectionId: binary.BigEndian.Uint32(pak.Value[8:]),
					Ttl:          binary.BigEndian.Uint32(pak.Value[8:]),
				})
			default:
				return &protocolError{"unknown dcp collection changed event version"}
			}
		default:
			return &protocolError{"unknown dcp event code"}
		}
	}

	return &protocolError{"unknown dcp event format"}
}

func (o UnsolicitedOpsParser) parseDcpStreamEnd(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpStreamEnd == nil {
		return errors.New("unhandled DcpStreamEnd event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	return handlers.DcpStreamEnd(&DcpStreamEndEvent{
		StreamId:  streamId,
		VbucketId: pak.VbucketID,
	})
}

func (o UnsolicitedOpsParser) parseDcpOsoSnapshot(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpOSOSnapshot == nil {
		return errors.New("unhandled DcpOSOSnapshot event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 4 {
		return handlers.DcpOSOSnapshot(&DcpOSOSnapshotEvent{
			StreamId:     streamId,
			VbucketId:    pak.VbucketID,
			SnapshotType: binary.BigEndian.Uint32(pak.Extras[0:]),
		})
	}

	return &protocolError{"unknown dcp oso snapshot format"}
}

func (o UnsolicitedOpsParser) parseDcpSeqNoAdvance(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpSeqNoAdvanced == nil {
		return errors.New("unhandled DcpSeqNoAdvanced event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 8 {
		return handlers.DcpSeqNoAdvanced(&DcpSeqNoAdvancedEvent{
			StreamId:  streamId,
			VbucketId: pak.VbucketID,
			SeqNo:     binary.BigEndian.Uint64(pak.Extras[0:]),
		})
	}

	return &protocolError{"unknown dcp seqno advanced format"}
}

func (o UnsolicitedOpsParser) parseSrvClustermapChangeNotification(pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.ClustermapChange == nil {
		return errors.New("unhandled ClustermapChange event")
	}

	if len(pak.Extras) == 4 {
		return handlers.ClustermapChange(&ClustermapChangeEvent{
			RevEpoch:   0,
			Rev:        int64(binary.BigEndian.Uint32(pak.Extras[0:])),
			BucketName: pak.Key,
			Config:     pak.Value,
		})
	} else if len(pak.Extras) == 16 {
		return handlers.ClustermapChange(&ClustermapChangeEvent{
			RevEpoch:   int64(binary.BigEndian.Uint64(pak.Extras[0:])),
			Rev:        int64(binary.BigEndian.Uint64(pak.Extras[8:])),
			BucketName: pak.Key,
			Config:     pak.Value,
		})
	}

	return &protocolError{"unknown clustermap change notification format"}
}

func (o UnsolicitedOpsParser) writeDcpNoOpResponse(r Replier, pak *Packet, _ *DcpNoOpEventResponse) error {
	return r.WritePacket(&Packet{
		IsResponse: true,
		OpCode:     OpCodeDcpNoOp,
		Opaque:     pak.Opaque,
		Status:     StatusSuccess,
	})
}

func (o UnsolicitedOpsParser) parseDcpNoOp(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpNoOp == nil {
		return errors.New("unhandled DcpNoOp event")
	}

	resp, err := handlers.DcpNoOp(&DcpNoOpEvent{})
	if err != nil {
		return err
	}

	if resp == nil {
		return nil
	}

	return o.writeDcpNoOpResponse(r, pak, resp)
}

func (o UnsolicitedOpsParser) Handle(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if !pak.IsResponse {
		switch pak.OpCode {
		case OpCodeSrvClustermapChangeNotification:
			return o.parseSrvClustermapChangeNotification(pak, handlers)
		case OpCodeDcpSnapshotMarker:
			return o.parseDcpSnapshotMarker(pak, handlers)
		case OpCodeDcpMutation:
			return o.parseDcpMutation(pak, handlers)
		case OpCodeDcpDeletion:
			return o.parseDcpDeletion(pak, handlers)
		case OpCodeDcpExpiration:
			return o.parseDcpExpiration(pak, handlers)
		case OpCodeDcpEvent:
			return o.parseDcpEvent(pak, handlers)
		case OpCodeDcpStreamEnd:
			return o.parseDcpStreamEnd(pak, handlers)
		case OpCodeDcpOsoSnapshot:
			return o.parseDcpOsoSnapshot(pak, handlers)
		case OpCodeDcpSeqNoAdvanced:
			return o.parseDcpSeqNoAdvance(pak, handlers)
		case OpCodeDcpNoOp:
			return o.parseDcpNoOp(r, pak, handlers)
		}
	}

	return &protocolError{
		fmt.Sprintf("unknown unsolicited event (opcode: %s)",
			pak.OpCode.String())}
}
