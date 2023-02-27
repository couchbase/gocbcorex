package cbqueryx

import (
	"errors"
	"fmt"
)

type QueryError struct {
	InnerError      error
	Statement       string
	ClientContextID string
	ErrorDescs      []QueryErrorDesc
	Endpoint        string
	// Uncommitted: This API may change in the future.
	ErrorsText string
	// Uncommitted: This API may change in the future.
	HTTPResponseCode int
}

func (e QueryError) Error() string {
	return e.InnerError.Error()
}

func (e QueryError) Unwrap() error {
	return e.InnerError
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

// Query Error Definitions RFC#58@15
var (
	ErrPlanningFailure = errors.New("planning failure")

	ErrIndexFailure = errors.New("index failure")

	ErrPreparedStatementFailure = errors.New("prepared statement failure")

	ErrDMLFailure = errors.New("data service returned an error during execution of DML statement")
)

var (
	ErrParsingFailure        = errors.New("parsing failure")
	ErrInternalServerError   = errors.New("internal server error")
	ErrAuthenticationFailure = errors.New("auth error")
	ErrCasMismatch           = errors.New("cas mismatch")
	ErrDocumentNotFound      = errors.New("doc not found")
	ErrDocumentExists        = errors.New("doc exists")
)

type InternalError struct {
	Reason error
}

func (e InternalError) Error() string {
	return fmt.Sprintf("internal error (%s)", e.Reason)
}

func (e InternalError) Unwrap() error {
	return e.Reason
}

var ErrInvalidArgument = errors.New("invalid argument")

type InvalidArgumentError struct {
	Message string
}

func (e InvalidArgumentError) Error() string {
	return fmt.Sprintf("invalid argument: %s", e.Message)
}

func (e InvalidArgumentError) Unwrap() error {
	return ErrInvalidArgument
}
