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
	DcpSeqNoAdvanced      func(req *DcpSeqoNoAdvancedEvent) error
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

func (o UnsolicitedOpsParser) parseDcpSnapshotMarker(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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
			StreamId:     streamId,
			Version:      1,
			StartSeqNo:   binary.BigEndian.Uint64(pak.Extras[0:]),
			EndSeqNo:     binary.BigEndian.Uint64(pak.Extras[8:]),
			SnapshotType: DcpSnapshotState(binary.BigEndian.Uint32(pak.Extras[16:])),
		})
	} else if len(pak.Extras) == 1 {
		// Length of 1 indicates v2+ packet
		version := pak.Extras[0]
		if version == 0 {
			return handlers.DcpSnapshotMarker(&DcpSnapshotMarkerEvent{
				StreamId:           streamId,
				Version:            2,
				StartSeqNo:         binary.BigEndian.Uint64(pak.Extras[0:]),
				EndSeqNo:           binary.BigEndian.Uint64(pak.Extras[8:]),
				SnapshotType:       DcpSnapshotState(binary.BigEndian.Uint32(pak.Extras[16:])),
				MaxVisibleSeqNo:    binary.BigEndian.Uint64(pak.Value[20:]),
				HighCompletedSeqNo: binary.BigEndian.Uint64(pak.Value[28:]),
			})
		} else if version == 1 {
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
		} else {
			return &protocolError{"unknown dcp snapshot marker version"}
		}
	} else {
		return &protocolError{"unknown dcp snapshot marker format"}
	}
}

func (o UnsolicitedOpsParser) parseDcpMutation(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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
			VbID:         pak.VbucketID,
			CollectionID: collectionId,
			Key:          key,
			Value:        pak.Value,
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpDeletion(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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
			VbID:         pak.VbucketID,
			CollectionID: collectionId,
			Key:          key,
		})
	} else if len(pak.Extras) == 21 {
		return handlers.DcpDeletion(&DcpDeletionEvent{
			StreamId:     streamId,
			Version:      2,
			SeqNo:        binary.BigEndian.Uint64(pak.Extras[0:]),
			RevNo:        binary.BigEndian.Uint64(pak.Extras[8:]),
			Cas:          pak.Cas,
			Datatype:     pak.Datatype,
			VbID:         pak.VbucketID,
			CollectionID: collectionId,
			Key:          key,
			DeleteTime:   binary.BigEndian.Uint32(pak.Extras[16:]),
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpExpiration(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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
			VbID:         pak.VbucketID,
			CollectionID: collectionId,
			Key:          key,
			DeleteTime:   binary.BigEndian.Uint32(pak.Extras[16:]),
		})
	}

	return &protocolError{"unknown dcp mutation format"}
}

func (o UnsolicitedOpsParser) parseDcpEvent(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 13 {
		seqNo := binary.BigEndian.Uint64(pak.Extras[0:])
		eventCode := DcpStreamEventCode(binary.BigEndian.Uint32(pak.Extras[8:]))
		version := pak.Extras[12]

		if eventCode == DcpStreamEventCodeCollectionCreate {
			if handlers.DcpCollectionCreation == nil {
				return errors.New("unhandled DcpCollectionCreation event")
			}

			if version == 0 {
				return handlers.DcpCollectionCreation(&DcpCollectionCreationEvent{
					StreamId:       streamId,
					Version:        0,
					SeqNo:          seqNo,
					VbID:           pak.VbucketID,
					ManifestUID:    binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:        binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionID:   binary.BigEndian.Uint32(pak.Value[12:]),
					CollectionName: string(pak.Key),
				})
			} else if version == 1 {
				return handlers.DcpCollectionCreation(&DcpCollectionCreationEvent{
					StreamId:       streamId,
					Version:        1,
					SeqNo:          seqNo,
					VbID:           pak.VbucketID,
					ManifestUID:    binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:        binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionID:   binary.BigEndian.Uint32(pak.Value[12:]),
					CollectionName: string(pak.Key),
					Ttl:            binary.BigEndian.Uint32(pak.Value[16:]),
				})
			} else {
				return &protocolError{"unknown dcp collection create event version"}
			}
		} else if eventCode == DcpStreamEventCodeCollectionDelete {
			if handlers.DcpCollectionDeletion == nil {
				return errors.New("unhandled DcpCollectionDeletion event")
			}

			if version == 0 {
				return handlers.DcpCollectionDeletion(&DcpCollectionDeletionEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbID:         pak.VbucketID,
					ManifestUID:  binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:      binary.BigEndian.Uint32(pak.Value[8:]),
					CollectionID: binary.BigEndian.Uint32(pak.Value[12:]),
				})
			} else {
				return &protocolError{"unknown dcp collection delete event version"}
			}
		} else if eventCode == DcpStreamEventCodeCollectionFlush {
			if handlers.DcpCollectionFlush == nil {
				return errors.New("unhandled DcpCollectionFlush event")
			}

			if version == 0 {
				return handlers.DcpCollectionFlush(&DcpCollectionFlushEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbID:         pak.VbucketID,
					ManifestUID:  binary.BigEndian.Uint64(pak.Value[0:]),
					CollectionID: binary.BigEndian.Uint32(pak.Value[8:]),
				})
			} else {
				return &protocolError{"unknown dcp collection flush event version"}
			}
		} else if eventCode == DcpStreamEventCodeScopeCreate {
			if handlers.DcpScopeCreation == nil {
				return errors.New("unhandled DcpScopeCreation event")
			}

			if version == 0 {
				return handlers.DcpScopeCreation(&DcpScopeCreationEvent{
					StreamId:    streamId,
					Version:     0,
					SeqNo:       seqNo,
					VbID:        pak.VbucketID,
					ManifestUID: binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:     binary.BigEndian.Uint32(pak.Value[8:]),
					ScopeName:   string(pak.Key),
				})
			} else {
				return &protocolError{"unknown dcp scope create event version"}
			}
		} else if eventCode == DcpStreamEventCodeScopeDelete {
			if handlers.DcpScopeDeletion == nil {
				return errors.New("unhandled DcpScopeDeletion event")
			}

			if version == 0 {
				return handlers.DcpScopeDeletion(&DcpScopeDeletionEvent{
					StreamId:    streamId,
					Version:     0,
					SeqNo:       seqNo,
					VbID:        pak.VbucketID,
					ManifestUID: binary.BigEndian.Uint64(pak.Value[0:]),
					ScopeID:     binary.BigEndian.Uint32(pak.Value[8:]),
				})
			} else {
				return &protocolError{"unknown dcp scope delete event version"}
			}
		} else if eventCode == DcpStreamEventCodeCollectionChanged {
			if handlers.DcpCollectionChanged == nil {
				return errors.New("unhandled DcpCollectionChanged event")
			}

			if version == 0 {
				return handlers.DcpCollectionChanged(&DcpCollectionModificationEvent{
					StreamId:     streamId,
					Version:      0,
					SeqNo:        seqNo,
					VbID:         pak.VbucketID,
					ManifestUID:  binary.BigEndian.Uint64(pak.Value[0:]),
					CollectionID: binary.BigEndian.Uint32(pak.Value[8:]),
					Ttl:          binary.BigEndian.Uint32(pak.Value[8:]),
				})
			} else {
				return &protocolError{"unknown dcp collection changed event version"}
			}
		} else {
			return &protocolError{"unknown dcp event code"}
		}
	}

	return &protocolError{"unknown dcp event format"}
}

func (o UnsolicitedOpsParser) parseDcpStreamEnd(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpStreamEnd == nil {
		return errors.New("unhandled DcpStreamEnd event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	return handlers.DcpStreamEnd(&DcpStreamEndEvent{
		StreamId: streamId,
		VbID:     pak.VbucketID,
	})
}

func (o UnsolicitedOpsParser) parseDcpOsoSnapshot(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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
			VbID:         pak.VbucketID,
			SnapshotType: binary.BigEndian.Uint32(pak.Extras[0:]),
		})
	}

	return &protocolError{"unknown dcp oso snapshot format"}
}

func (o UnsolicitedOpsParser) parseDcpSeqNoAdvance(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
	if handlers.DcpSeqNoAdvanced == nil {
		return errors.New("unhandled DcpSeqNoAdvanced event")
	}

	streamId, err := o.decodeExtFrames(pak.FramingExtras)
	if err != nil {
		return err
	}

	if len(pak.Extras) == 4 {
		return handlers.DcpSeqNoAdvanced(&DcpSeqoNoAdvancedEvent{
			StreamId: streamId,
			VbID:     pak.VbucketID,
			SeqNo:    binary.BigEndian.Uint64(pak.Extras[0:]),
		})
	}

	return &protocolError{"unknown dcp oso snapshot format"}
}

func (o UnsolicitedOpsParser) parseSrvClustermapChangeNotification(r Replier, pak *Packet, handlers *UnsolicitedOpsHandlers) error {
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

func (o UnsolicitedOpsParser) writeDcpNoOpResponse(r Replier, pak *Packet, resp *DcpNoOpEventResponse) error {
	if resp == nil {
		return nil
	}

	return r.WritePacket(&Packet{
		Magic:  MagicRes,
		OpCode: OpCodeDcpNoOp,
		Opaque: pak.Opaque,
		Status: StatusSuccess,
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
	if pak.Magic.IsRequest() {
		if pak.Magic.IsServerInitiated() {
			if pak.OpCode == OpCodeSrvClustermapChangeNotification {
				return o.parseSrvClustermapChangeNotification(r, pak, handlers)
			}
		} else {
			if pak.OpCode == OpCodeDcpSnapshotMarker {
				return o.parseDcpSnapshotMarker(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpMutation {
				return o.parseDcpMutation(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpDeletion {
				return o.parseDcpDeletion(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpExpiration {
				return o.parseDcpExpiration(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpEvent {
				return o.parseDcpEvent(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpStreamEnd {
				return o.parseDcpStreamEnd(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpOsoSnapshot {
				return o.parseDcpOsoSnapshot(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpSeqNoAdvanced {
				return o.parseDcpSeqNoAdvance(r, pak, handlers)
			} else if pak.OpCode == OpCodeDcpNoOp {
				return o.parseDcpNoOp(r, pak, handlers)
			}
		}
	}

	return &protocolError{
		fmt.Sprintf("unknown unsolicited event (opcode: %s)",
			pak.OpCode.String(pak.Magic))}
}
