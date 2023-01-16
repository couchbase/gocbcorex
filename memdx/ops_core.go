package memdx

import (
	"encoding/binary"
	"errors"
	"strings"
)

type OpsCore struct {
	Dispatcher Dispatcher
}

func (o OpsCore) decodeError(resp *Packet) error {
	// TODO(brett19): Do better...
	return errors.New("generic error: " + resp.Status.String())
}

type HelloRequest struct {
	ClientName        []byte
	RequestedFeatures []HelloFeature
}

type HelloResponse struct {
	EnabledFeatures []HelloFeature
}

func (o OpsCore) Hello(req *HelloRequest, cb func(*HelloResponse, error)) error {
	featureBytes := make([]byte, len(req.RequestedFeatures)*2)
	for featIdx, featCode := range req.RequestedFeatures {
		binary.BigEndian.PutUint16(featureBytes[featIdx*2:], uint16(featCode))
	}

	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeHello,
		Key:    req.ClientName,
		Value:  featureBytes,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		if len(resp.Value)%2 != 0 {
			cb(nil, protocolError{"invalid hello features length"})
			return false
		}

		numFeats := len(resp.Value) / 2
		features := make([]HelloFeature, numFeats)
		for featIdx := range features {
			features[featIdx] = HelloFeature(binary.BigEndian.Uint16(resp.Value[featIdx*2:]))
		}

		cb(&HelloResponse{
			EnabledFeatures: features,
		}, nil)
		return false
	})
}

func (o OpsCore) GetErrorMap(cb func([]byte, error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetErrorMap,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(resp.Value, nil)
		return false
	})
}

type GetClusterConfigRequest struct {
	BucketName []byte
}

func (o OpsCore) GetClusterConfig(req *GetClusterConfigRequest, cb func([]byte, error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetClusterConfig,
		Key:    req.BucketName,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(resp.Value, nil)
		return false
	})
}

type SelectBucketRequest struct {
	BucketName []byte
}

func (o OpsCore) SelectBucket(req *SelectBucketRequest, cb func(error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSelectBucket,
		Key:    req.BucketName,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			cb(ErrUnknownBucketName)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(o.decodeError(resp))
			return false
		}

		cb(nil)
		return false
	})
}

type SASLListMechsResponse struct {
	AvailableMechs []string
}

func (o OpsCore) SASLListMechs(cb func(*SASLListMechsResponse, error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLListMechs,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		mechsList := string(resp.Value)
		mechsArr := strings.Split(mechsList, " ")

		cb(&SASLListMechsResponse{
			AvailableMechs: mechsArr,
		}, nil)
		return false
	})
}

type SASLAuthRequest struct {
	Mechanism string
	Payload   []byte
}

type SASLAuthResponse struct {
	Payload []byte
}

func (o OpsCore) SASLAuth(req *SASLAuthRequest, cb func(*SASLAuthResponse, error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLAuth,
		Key:    []byte(req.Mechanism),
		Value:  req.Payload,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&SASLAuthResponse{
			Payload: resp.Value,
		}, nil)
		return false
	})
}

type SASLStepRequest struct {
	Mechanism string
	Payload   []byte
}

type SASLStepResponse struct {
	Payload []byte
}

func (o OpsCore) SASLStep(req *SASLStepRequest, cb func(*SASLStepResponse, error)) error {
	return o.Dispatcher.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLStep,
		Key:    []byte(req.Mechanism),
		Value:  req.Payload,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&SASLStepResponse{
			Payload: resp.Value,
		}, nil)
		return false
	})
}
