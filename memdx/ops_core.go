package memdx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strings"
)

type OpsCore struct {
}

func (o OpsCore) decodeErrorContext(resp *Packet, err error, dispatchedTo string, dispatchedFrom string) error {
	baseCause := ServerError{
		Cause:          err,
		DispatchedTo:   dispatchedTo,
		DispatchedFrom: dispatchedFrom,
		Opaque:         resp.Opaque,
	}

	if len(resp.Value) > 0 {
		if resp.Status == StatusNotMyVBucket {
			return ServerErrorWithConfig{
				Cause:      baseCause,
				ConfigJson: resp.Value,
			}
		}

		return ServerErrorWithContext{
			Cause:       baseCause,
			ContextJson: resp.Value,
		}
	}

	return baseCause
}

func (o OpsCore) decodeError(resp *Packet, dispatchedTo string, dispatchedFrom string) error {
	var err error
	if resp.Status == StatusNotMyVBucket {
		err = ErrNotMyVbucket
	} else {
		err = errors.New("unexpected status: " + resp.Status.String())
	}

	return o.decodeErrorContext(resp, err, dispatchedTo, dispatchedFrom)
}

type HelloRequest struct {
	ClientName        []byte
	RequestedFeatures []HelloFeature
}

type HelloResponse struct {
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
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
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
	Version uint16
}

func (o OpsCore) GetErrorMap(d Dispatcher, req *GetErrorMapRequest, cb func([]byte, error)) (PendingOp, error) {
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
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(resp.Value, nil)
		return false
	})
}

type GetClusterConfigRequest struct{}

func (o OpsCore) GetClusterConfig(d Dispatcher, req *GetClusterConfigRequest, cb func([]byte, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeGetClusterConfig,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		host, _, _ := net.SplitHostPort(d.RemoteAddr())
		if host == "" {
			cb(nil, errors.New("failed to identify memd hostname for $HOST replacement"))
		}

		outValue := bytes.ReplaceAll(resp.Value, []byte("$HOST"), []byte(host))

		cb(outValue, nil)
		return false
	})
}

type SelectBucketRequest struct {
	BucketName string
}

func (o OpsCore) SelectBucket(d Dispatcher, req *SelectBucketRequest, cb func(error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSelectBucket,
		Key:    []byte(req.BucketName),
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(err)
			return false
		}

		if resp.Status == StatusAccessError {
			cb(ErrUnknownBucketName)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(nil)
		return false
	})
}

type SASLListMechsResponse struct {
	AvailableMechs []AuthMechanism
}

func (o OpsCore) SASLListMechs(d Dispatcher, cb func(*SASLListMechsResponse, error)) (PendingOp, error) {
	return d.Dispatch(&Packet{
		Magic:  MagicReq,
		OpCode: OpCodeSASLListMechs,
	}, func(resp *Packet, err error) bool {
		if err != nil {
			cb(nil, err)
			return false
		}

		if resp.Status != StatusSuccess {
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
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
	Mechanism AuthMechanism
	Payload   []byte
}

type SASLAuthResponse struct {
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
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
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
	Mechanism AuthMechanism
	Payload   []byte
}

type SASLStepResponse struct {
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
			cb(nil, o.decodeError(resp, d.RemoteAddr(), d.LocalAddr()))
			return false
		}

		cb(&SASLStepResponse{
			NeedsMoreSteps: false,
			Payload:        resp.Value,
		}, nil)
		return false
	})
}
