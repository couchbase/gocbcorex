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
	ErrIndexExists              = errors.New("index exists")
	ErrIndexNotFound            = errors.New("index not found")
	ErrWriteInReadOnlyQuery     = errors.New("write statement used in a read-only query")
	ErrKeyspaceNotFound         = errors.New("keyspace not found")
	ErrScopeNotFound            = errors.New("scope not found")
	ErrServerInvalidArg         = errors.New("invalid argument")
	ErrBuildAlreadyInProgress   = errors.New("build already in progress")
	ErrBuildFails               = errors.New("build fails, will be retried by query engine")

	ErrBucketNotFound     = fmt.Errorf("bucket not found: %w", ErrKeyspaceNotFound)
	ErrCollectionNotFound = fmt.Errorf("collection not found: %w", ErrKeyspaceNotFound)
)

type Error struct {
	Cause error

	StatusCode      int
	Endpoint        string
	Statement       string
	ClientContextId string
	ErrorDescs      []ErrorDesc
}

func (e Error) Error() string {
	return fmt.Sprintf("query server error: %s", e.Cause.Error())
}

func (e Error) Unwrap() error {
	return e.Cause
}

// ErrorDesc represents specific n1ql error data.
type ErrorDesc struct {
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

type ServerError struct {
	InnerError error
	Code       uint32
	Msg        string
}

func (e ServerError) Error() string {
	return fmt.Sprintf("query error: %s (code: %d, msg: %s)",
		e.InnerError.Error(),
		e.Code, e.Msg)
}

func (e ServerError) Unwrap() error {
	return e.InnerError
}

type ServerErrors struct {
	Errors []*ServerError
}

func (e ServerErrors) Error() string {
	return fmt.Sprintf("%s (+ %d other errors)", e.Errors[0].Error(), len(e.Errors)-1)
}

func (e ServerErrors) Unwrap() error {
	return e.Errors[0]
}

type ServerInvalidArgError struct {
	Argument string
	Reason   string
}

func (e ServerInvalidArgError) Unwrap() error {
	return ErrServerInvalidArg
}

func (e ServerInvalidArgError) Error() string {
	return fmt.Sprintf("server invalid arg: %s (argument: %s, reason: %s)", e.Unwrap().Error(), e.Argument, e.Reason)
}

type ResourceError struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	Cause          error
}

func (e ResourceError) Unwrap() error {
	return e.Cause
}

func (e ResourceError) Error() string {
	if e.CollectionName == "" && e.ScopeName == "" {
		return fmt.Sprintf("resource error: %s (bucket: %s)", e.Unwrap().Error(), e.BucketName)
	}
	if e.CollectionName == "" {
		return fmt.Sprintf("resource error: %s (bucket: %s, scope: %s)", e.Unwrap().Error(), e.BucketName, e.ScopeName)
	}
	return fmt.Sprintf("resource error: %s (bucket: %s, scope: %s, collection: %s)", e.Unwrap().Error(), e.BucketName, e.ScopeName, e.CollectionName)
}
