package cbqueryx

import (
	"errors"
	"fmt"
)

var (
	ErrParsingFailure           = errors.New("parsing failure")
	ErrInternalServerError      = errors.New("internal server error")
	ErrAuthenticationFailure    = errors.New("auth error")
	ErrCasMismatch              = errors.New("cas mismatch")
	ErrDocumentNotFound         = errors.New("doc not found")
	ErrDocumentExists           = errors.New("doc exists")
	ErrPlanningFailure          = errors.New("planning failure")
	ErrIndexFailure             = errors.New("index failure")
	ErrPreparedStatementFailure = errors.New("prepared statement failure")
	ErrDmlFailure               = errors.New("data service returned an error during execution of DML statement")
	ErrTimeout                  = errors.New("timeout")
)

type QueryError struct {
	Cause error

	StatusCode      int
	Endpoint        string
	Statement       string
	ClientContextId string
	ErrorDescs      []QueryErrorDesc
}

func (e QueryError) Error() string {
	return fmt.Sprintf("query server error: %s", e.Cause.Error())
}

func (e QueryError) Unwrap() error {
	return e.Cause
}

// QueryErrorDesc represents specific n1ql error data.
type QueryErrorDesc struct {
	// Error is populated if the SDK understand what this error desc is.
	Error   error
	Code    uint32
	Message string
	Retry   bool
	Reason  map[string]interface{}
}

type contextualError struct {
	Cause       error
	Description string
}

func (e contextualError) Error() string {
	return e.Description + ": " + e.Cause.Error()
}

func (e contextualError) Unwrap() error {
	return e.Cause
}

type QueryServerError struct {
	InnerError error
	Code       uint32
	Msg        string
}

func (e QueryServerError) Error() string {
	return fmt.Sprintf("query error: %s (code: %d, msg: %s)",
		e.InnerError.Error(),
		e.Code, e.Msg)
}

func (e QueryServerError) Unwrap() error {
	return e.InnerError
}

type QueryServerErrors struct {
	Errors []*QueryServerError
}

func (e QueryServerErrors) Error() string {
	return fmt.Sprintf("%s (+ %d other errors)", e.Errors[0].Error(), len(e.Errors)-1)
}

func (e QueryServerErrors) Unwrap() error {
	return e.Errors[0]
}
