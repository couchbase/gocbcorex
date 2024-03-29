package cbmgmtx

import (
	"errors"
	"fmt"
)

var (
	ErrAccessDenied       = errors.New("access denied")
	ErrUnsupportedFeature = errors.New("unsupported feature")
	ErrScopeExists        = errors.New("scope exists")
	ErrScopeNotFound      = errors.New("scope not found")
	ErrCollectionExists   = errors.New("collection exists")
	ErrCollectionNotFound = errors.New("collection not found")
	ErrBucketExists       = errors.New("bucket exists")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrFlushDisabled      = errors.New("flush is disabled")
	ErrServerInvalidArg   = errors.New("invalid argument")
	ErrBucketUuidMismatch = errors.New("bucket uuid mismatch")
	ErrUserNotFound       = errors.New("user not found")
	ErrManifestOutOfDate  = errors.New("manifest out of date")
	ErrOperationDelayed   = errors.New("operation was delayed, but will continue")
)

type ServerError struct {
	Cause      error
	StatusCode int
	Body       []byte
}

func (e ServerError) Error() string {
	return fmt.Sprintf("server error: %s (status: %d, body: `%s`)", e.Cause.Error(), e.StatusCode, e.Body)
}

func (e ServerError) Unwrap() error {
	return e.Cause
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

type ServerInvalidArgError struct {
	Argument string
	Reason   string
}

func (e ServerInvalidArgError) Unwrap() error {
	return ErrServerInvalidArg
}

func (e ServerInvalidArgError) Error() string {
	return fmt.Sprintf("%s: %s - %s", e.Unwrap().Error(), e.Argument, e.Reason)
}

type ResourceError struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	Cause          error
}

func (e ResourceError) Unwrap() error {
	return e.Cause
}

func (e ResourceError) Error() string {
	if e.CollectionName == "" && e.ScopeName == "" {
		return fmt.Sprintf("%s - '%s'", e.Unwrap().Error(), e.BucketName)
	}
	if e.CollectionName == "" {
		return fmt.Sprintf("%s - '%s/%s'", e.Unwrap().Error(), e.BucketName, e.ScopeName)
	}
	return fmt.Sprintf("%s - '%s/%s/%s'", e.Unwrap().Error(), e.BucketName, e.ScopeName, e.CollectionName)
}
