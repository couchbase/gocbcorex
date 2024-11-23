package memdx

import (
	"encoding/binary"
	"errors"
	"strings"
)

type OpRequest interface {
	OpName() string
}

type OpResponse interface {
	comparable
}

type CoreRequestMeta struct{}

type CoreResponseMeta struct {
	// Note that while the server technically supports server durations for some
	// of these core commands, the client needs to enable durations first, so we
	// ignore that capability here.
}

type OpsCore struct {
}

func (o OpsCore) decodeErrorContext(resp *Packet, err error) error {
	baseCause := &ServerError{
		OpCode: resp.OpCode,
		Status: resp.Status,
		Cause:  err,
		Opaque: resp.Opaque,
	}

	if len(resp.Value) > 0 {
		if resp.Status == StatusNotMyVBucket {
			return &ServerErrorWithConfig{
				Cause:      *baseCause,
				ConfigJson: resp.Value,
			}
		}

		return &ServerErrorWithContext{
			Cause:       *baseCause,
			ContextJson: resp.Value,
		}
	}

	return baseCause
}

func (o OpsCore) decodeError(resp *Packet) error {
	var err error
	if resp.Status == StatusNotMyVBucket {
		err = ErrNotMyVbucket
	} else if resp.Status == StatusTmpFail {
		err = ErrTmpFail
	} else {
		err = errors.New("unexpected status: " + resp.Status.String())
	}

	return o.decodeErrorContext(resp, err)
}

type HelloRequest struct {
	CoreRequestMeta
	ClientName        []byte
	RequestedFeatures []HelloFeature
}

func (r HelloRequest) OpName() string { return OpCodeHello.String() }

type HelloResponse struct {
	CoreResponseMeta
	EnabledFeatures []HelloFeature
}

func (o OpsCore) Hello(d Dispatcher, req *HelloRequest, cb func(*HelloResponse, error)) (PendingOp, error) {
	featureBytes := make([]byte, len(req.RequestedFeatures)*2)
	for featIdx, featCode := range req.RequestedFeatures {
		binary.BigEndian.PutUint16(featureBytes[featIdx*2:], uint16(featCode))
	}

	return d.Dispatch(&Packet{
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

type GetErrorMapRequest struct {
	CoreRequestMeta
	Version uint16
}

func (r GetErrorMapRequest) OpName() string { return OpCodeGetErrorMap.String() }

type GetErrorMapResponse struct {
	CoreResponseMeta
	ErrorMap []byte
}

func (o OpsCore) GetErrorMap(d Dispatcher, req *GetErrorMapRequest, cb func(*GetErrorMapResponse, error)) (PendingOp, error) {
	valueBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(valueBuf[0:], req.Version)

	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetErrorMap,
		Value:  valueBuf,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&GetErrorMapResponse{
			ErrorMap: resp.Value,
		}, nil)
		return false
	})
}

type GetClusterConfigRequest struct {
	CoreRequestMeta
}

func (r GetClusterConfigRequest) OpName() string { return OpCodeGetClusterConfig.String() }

type GetClusterConfigResponse struct {
	CoreResponseMeta
	Config []byte
}

func (o OpsCore) GetClusterConfig(d Dispatcher, req *GetClusterConfigRequest, cb func(*GetClusterConfigResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetClusterConfig,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusKeyNotFound {
			// KeyNotFound appears here when the bucket was initialized by ns_server, but
			// ns_server has not posted a configuration for the bucket to kv_engine yet. We
			// transform this into a ErrTmpFail as we make the assumption that the
			// SelectBucket will have failed if this was anything but a transient issue.
			cb(nil, ErrConfigNotSet)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&GetClusterConfigResponse{
			Config: resp.Value,
		}, nil)
		return false
	})
}

type SelectBucketRequest struct {
	CoreRequestMeta
	BucketName string
}

func (r SelectBucketRequest) OpName() string { return OpCodeSelectBucket.String() }

type SelectBucketResponse struct {
	CoreResponseMeta
}

func (o OpsCore) SelectBucket(d Dispatcher, req *SelectBucketRequest, cb func(*SelectBucketResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSelectBucket,
		Key:    []byte(req.BucketName),
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusAccessError {
			cb(nil, ErrUnknownBucketName)
			return false
		} else if resp.Status == StatusKeyNotFound {
			// in some cases, it seems that kv_engine returns KeyNotFound rather
			// than access error when a bucket does not exist yet.	I believe this
			// is a race between the RBAC data updating and the bucket data updating.
			cb(nil, ErrUnknownBucketName)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&SelectBucketResponse{}, nil)
		return false
	})
}

type SASLListMechsRequest struct {
	CoreRequestMeta
}

func (r SASLListMechsRequest) OpName() string { return OpCodeSASLListMechs.String() }

type SASLListMechsResponse struct {
	CoreResponseMeta
	AvailableMechs []AuthMechanism
}

func (o OpsCore) SASLListMechs(d Dispatcher, req *SASLListMechsRequest, cb func(*SASLListMechsResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
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
		mechsStrArr := strings.Split(mechsList, " ")
		mechsArr := make([]AuthMechanism, len(mechsStrArr))
		for i, mech := range mechsStrArr {
			mechsArr[i] = AuthMechanism(mech)
		}

		cb(&SASLListMechsResponse{
			AvailableMechs: mechsArr,
		}, nil)
		return false
	})
}

type SASLAuthRequest struct {
	CoreRequestMeta
	Mechanism AuthMechanism
	Payload   []byte
}

func (r SASLAuthRequest) OpName() string { return OpCodeSASLAuth.String() }

type SASLAuthResponse struct {
	CoreResponseMeta
	NeedsMoreSteps bool
	Payload        []byte
}

func (o OpsCore) SASLAuth(d Dispatcher, req *SASLAuthRequest, cb func(*SASLAuthResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLAuth,
		Key:    []byte(req.Mechanism),
		Value:  req.Payload,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusAuthContinue {
			cb(&SASLAuthResponse{
				NeedsMoreSteps: true,
				Payload:        resp.Value,
			}, nil)
			return false
		} else if resp.Status == StatusAuthError {
			cb(nil, ErrAuthError)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&SASLAuthResponse{
			NeedsMoreSteps: false,
			Payload:        resp.Value,
		}, nil)
		return false
	})
}

type SASLStepRequest struct {
	CoreRequestMeta
	Mechanism AuthMechanism
	Payload   []byte
}

func (r SASLStepRequest) OpName() string { return OpCodeSASLStep.String() }

type SASLStepResponse struct {
	CoreResponseMeta
	NeedsMoreSteps bool
	Payload        []byte
}

func (o OpsCore) SASLStep(d Dispatcher, req *SASLStepRequest, cb func(*SASLStepResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLStep,
		Key:    []byte(req.Mechanism),
		Value:  req.Payload,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status == StatusAuthContinue {
			cb(&SASLStepResponse{
				NeedsMoreSteps: true,
				Payload:        resp.Value,
			}, nil)
			return false
		} else if resp.Status == StatusAuthError {
			cb(nil, ErrAuthError)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&SASLStepResponse{
			NeedsMoreSteps: false,
			Payload:        resp.Value,
		}, nil)
		return false
	})
}

type NoOpRequest struct {
	CoreRequestMeta
}

func (r NoOpRequest) OpName() string { return OpCodeNoOp.String() }

type NoOpResponse struct {
	CoreResponseMeta
}

func (o OpsCore) NoOp(d Dispatcher, req *NoOpRequest, cb func(*NoOpResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeNoOp,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp))
			return false
		}

		cb(&NoOpResponse{}, nil)
		return false
	})
}
