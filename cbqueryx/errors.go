package cbqueryx

import (
	"errors"
	"fmt"
	"strings"
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
	ErrConcurrentOperation      = errors.New("concurrent operation")

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
	Retry      bool
	Line       uint32
	Column     uint32

	// Cause is the normalized, fully-decoded cause chain that the query service
	// returned alongside this error (from the wire "reason", or "cause" for
	// transaction errors). It is nil when the server returned no cause. It
	// preserves all detail the query service propagated - including nested
	// KV/data-service errors - for debugging. See QueryErrorCause.
	Cause *QueryErrorCause
}

func (e ServerError) Error() string {
	msg := fmt.Sprintf("query error: %s (code: %d, msg: %s)",
		e.InnerError.Error(),
		e.Code, e.Msg)
	if e.Cause != nil {
		msg += fmt.Sprintf(" (cause: %s)", e.Cause.chainString())
	}
	return msg
}

func (e ServerError) Unwrap() error {
	return e.InnerError
}

// QueryErrorCause is a single, normalized node in a query error's cause chain.
//
// The query service's wire format for causes is inconsistent: the same logical
// cause can appear under "reason" (top level), "cause" (nested and transaction
// errors), or "error" (KV detail maps), and a node may be a full nested error
// object or a bare string. QueryErrorCause papers over this to provide simple
// structured access to the useful parts of the chain. It deliberately does not
// capture every incidental field the query service emits - if the full detail
// is needed it is available from the raw response JSON at a higher level.
//
// When a node is a bare string (e.g. the KV status "SYNC_WRITE_AMBIGUOUS"), its
// text is captured in Message.
type QueryErrorCause struct {
	Code    uint32
	Key     string
	Message string

	// Cause is the next node down the chain, normalized identically. It is
	// populated by descending through this node's "cause" field, or failing
	// that its "error" field, matching the query service's own cause traversal.
	Cause *QueryErrorCause
}

// hasCode reports whether this node or any node beneath it carries the given
// error code.
func (c *QueryErrorCause) hasCode(code uint32) bool {
	for cur := c; cur != nil; cur = cur.Cause {
		if cur.Code == code {
			return true
		}
	}
	return false
}

// chainString renders the whole cause chain into a compact single-line string
// suitable for debug logging. Nodes that carry no useful content (bare link
// nodes) are skipped.
func (c *QueryErrorCause) chainString() string {
	var parts []string
	for cur := c; cur != nil; cur = cur.Cause {
		if s := cur.nodeString(); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " -> ")
}

func (c *QueryErrorCause) nodeString() string {
	switch {
	case c.Code != 0 && c.Message != "":
		return fmt.Sprintf("%d: %s", c.Code, c.Message)
	case c.Code != 0:
		return fmt.Sprintf("code %d", c.Code)
	default:
		return c.Message
	}
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
