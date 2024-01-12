package jsonrpcx

import (
	"encoding/json"
)

func MakeErrorResponse(req *Request, rerr *Error) *Response {
	return &Response{
		ID:    req.ID,
		Error: rerr,
	}
}

func ProcUnknownMethod(req *Request, fn func(method string, params interface{})) *Response {
	var params interface{}
	err := json.Unmarshal(req.Params, &params)
	if err != nil {
		fn(req.Method, nil)
	} else {
		fn(req.Method, params)
	}
	return &Response{
		ID: req.ID,
		Error: &Error{
			Code:    -32602,
			Message: "Method not found",
		},
	}
}

func Proc1ArgMethod[T0 any, TR any](req *Request, fn func(param0 T0) (TR, error)) *Response {
	var params []json.RawMessage
	err := json.Unmarshal(req.Params, &params)
	if err != nil {
		// we currently only support positional arguments
		return MakeErrorResponse(req, &Error{
			Code:    -32600,
			Message: "Invalid Request",
			Data: map[string]string{
				"details": "only positional arguments are supported",
			},
		})
	}

	if len(params) != 1 {
		return MakeErrorResponse(req, &Error{
			Code:    -32602,
			Message: "Invalid params",
			Data: map[string]string{
				"details": "method accepts exactly 1 parameter",
			},
		})
	}

	var param0 T0
	err = json.Unmarshal(params[0], &param0)
	if err != nil {
		return MakeErrorResponse(req, &Error{
			Code:    -32602,
			Message: "Invalid params",
			Data: map[string]string{
				"details": "failed to parse parameter 0",
			},
		})
	}

	result, err := fn(param0)
	if err != nil {
		return MakeErrorResponse(req, &Error{
			Code:    -32000,
			Message: "Request failed",
			Data: map[string]string{
				"details": err.Error(),
			},
		})
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return MakeErrorResponse(req, &Error{
			Code:    -32000,
			Message: "Request failed",
			Data: map[string]string{
				"details": err.Error(),
			},
		})
	}

	return &Response{
		ID:     req.ID,
		Result: resultBytes,
	}
}
