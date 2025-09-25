package gocbcorex

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrParsingFailure             = errors.New("parsing failure")
	ErrInternalServerError        = errors.New("internal server error")
	ErrVbucketMapOutdated         = errors.New("the vbucket map is out of date")
	ErrCollectionManifestOutdated = errors.New("the collection manifest is out of date")
	ErrCollectionIDMismatch       = errors.New("the provided collection id does not match")
	ErrServiceNotAvailable        = errors.New("service is not available")
	ErrNoBucketSelected           = errors.New("no bucket selected, please select a bucket before performing bucket operations")
)

type placeholderError struct {
	Inner string
}

func (pe placeholderError) Error() string {
	return pe.Inner
}

type CoreError struct {
	InnerError error
	Context    string
}

func (e CoreError) Error() string {
	return e.InnerError.Error()
}

type CollectionNotFoundError struct {
	CoreError
	ManifestUid uint64
}

func (e CollectionNotFoundError) Error() string {
	return e.InnerError.Error()
}

type CollectionManifestOutdatedError struct {
	Cause             error
	ManifestUid       uint64
	ServerManifestUid uint64
}

func (e CollectionManifestOutdatedError) Error() string {
	return fmt.Sprintf("collection manifest outdated: our manifest uid: %d, server manifest uid: %d", e.ManifestUid, e.ServerManifestUid)
}

func (e CollectionManifestOutdatedError) Unwrap() error {
	return ErrCollectionManifestOutdated
}

type CollectionIDMismatchError struct {
	CollectionID       uint32
	ServerCollectionID uint32
	ManifestUid        uint64
}

func (e CollectionIDMismatchError) Error() string {
	return fmt.Sprintf("provided collection id mismatch: provided collection id: %d, sdk collection id: %d, "+
		"manifest uid: %d", e.CollectionID, e.ServerCollectionID, e.ManifestUid)
}

func (e CollectionIDMismatchError) Unwrap() error {
	return ErrCollectionIDMismatch
}

type VbucketMapOutdatedError struct {
	Cause error
}

func (e VbucketMapOutdatedError) Error() string {
	return "vbucket map outdated"
}

func (e VbucketMapOutdatedError) Unwrap() error {
	return ErrVbucketMapOutdated
}

type contextualDeadline struct {
	Message string
}

func (e contextualDeadline) Error() string {
	return e.Message + ": " + context.DeadlineExceeded.Error()
}

func (e contextualDeadline) Unwrap() error {
	return context.DeadlineExceeded
}

type retrierDeadlineError struct {
	Cause      error
	RetryCause error
	RetryIdx   int
}

func (e retrierDeadlineError) Error() string {
	if e.RetryCause != nil {
		return fmt.Sprintf("timed out in retrier: %s (attempt: %d, retry cause: %s)", e.Cause, e.RetryIdx, e.RetryCause)
	} else {
		return fmt.Sprintf("timed out in retrier: %s (attempt: %d)", e.Cause, e.RetryIdx)
	}
}

func (e retrierDeadlineError) Unwrap() error {
	return e.Cause
}

var ErrInvalidVbucket = errors.New("invalid vbucket")

type invalidVbucketError struct {
	RequestedVbId uint16
	NumVbuckets   uint16
}

func (e invalidVbucketError) Error() string {
	return fmt.Sprintf("invalid vbucket requested (%d >= %d)", e.RequestedVbId, e.NumVbuckets)
}

func (e invalidVbucketError) Unwrap() error {
	return ErrInvalidVbucket
}

var ErrInvalidReplica = errors.New("invalid replica")

type invalidReplicaError struct {
	RequestedReplica uint32
	NumServers       uint32
}

func (e invalidReplicaError) Error() string {
	return fmt.Sprintf("invalid replica requested (%d >= %d)", e.RequestedReplica, e.NumServers)
}

func (e invalidReplicaError) Unwrap() error {
	return ErrInvalidReplica
}

var ErrNoServerAssigned = errors.New("no server assigned to vbucket")

type noServerAssignedError struct {
	RequestedVbId uint16
}

func (e noServerAssignedError) Error() string {
	return fmt.Sprintf("vbucket %d has no assigned server", e.RequestedVbId)
}

func (e noServerAssignedError) Unwrap() error {
	return ErrNoServerAssigned
}

var ErrInvalidArgument = errors.New("invalid argument")

var ErrBootstrapAllFailed = errors.New("all bootstrap hosts failed")

type BootstrapAllFailedError struct {
	Errors map[string]error
}

func (e BootstrapAllFailedError) Error() string {
	var errStrs []string
	for endpoint, err := range e.Errors {
		errStrs = append(errStrs, fmt.Sprintf("%s: {%s}", endpoint, err.Error()))
	}
	return fmt.Sprintf("all bootstrap hosts failed (%s)", strings.Join(errStrs, ", "))
}

func (e BootstrapAllFailedError) Unwrap() error {
	return ErrBootstrapAllFailed
}

type contextualError struct {
	Message string
	Cause   error
}

func (e contextualError) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.Cause.Error())
}

func (e contextualError) Unwrap() error {
	return e.Cause
}

type serviceNotAvailableError struct {
	Service ServiceType
}

func (e serviceNotAvailableError) Error() string {
	return strings.ToLower(e.Service.String()) + " service not available"
}

func (e serviceNotAvailableError) Unwrap() error {
	return ErrServiceNotAvailable
}

type PathProjectionError struct {
	Path  string
	Cause error
}

func (e PathProjectionError) Error() string {
	return fmt.Sprintf("%s (path: %s)", e.Cause, e.Path)
}

func (e PathProjectionError) Unwrap() error {
	return e.Cause
}

type KvBucketError struct {
	Cause      error
	BucketName string
}

func (e KvBucketError) Error() string {
	return fmt.Sprintf("%s (bucket: %s)", e.Cause, e.BucketName)
}

func (e KvBucketError) Unwrap() error {
	return e.Cause
}

type RetryOrchestrationError struct {
	Cause         error
	OriginalCause error
}

func (e RetryOrchestrationError) Error() string {
	return fmt.Sprintf("error occured deciding whether to retry operation: %s (original error: %s)", e.Cause, e.OriginalCause)
}

func (e RetryOrchestrationError) Unwrap() error {
	return e.Cause
}
