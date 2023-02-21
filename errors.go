package gocbcorex

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex/memdx"
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

type ServerManifestOutdatedError struct {
	ManifestUid       uint64
	ServerManifestUid uint64
}

func (e ServerManifestOutdatedError) Error() string {
	return fmt.Sprintf("server manifest outdated: our manifest uid: %d, server manifest uid: %d", e.ManifestUid, e.ServerManifestUid)
}

func (e ServerManifestOutdatedError) Unwrap() error {
	return memdx.ErrUnknownCollectionID
}

type contextualDeadline struct {
	Message string
}

func (e contextualDeadline) Error() string {
	return e.Message
}

func (e contextualDeadline) Unwrap() error {
	return context.DeadlineExceeded
}

type retrierDeadlineError struct {
	Cause      error
	RetryCause error
}

func (e retrierDeadlineError) Error() string {
	if e.RetryCause != nil {
		return fmt.Sprintf("timed out during retrying: %s (retry cause: %s)", e.Cause, e.RetryCause)
	} else {
		return fmt.Sprintf("timed out during retrying: %s", e.Cause)
	}
}

func (e retrierDeadlineError) Unwrap() error {
	return e.Cause
}

type illegalStateError struct {
	Message string
}

func (e illegalStateError) Error() string {
	return fmt.Sprintf("illegal state: %s", e.Message)
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
