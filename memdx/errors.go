package memdx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

var (
	ErrUnknownBucketName                   = errors.New("unknown bucket name")
	ErrUnknownCollectionID                 = errors.New("unknown collection id")
	ErrUnknownScopeName                    = errors.New("unknown scope name")
	ErrUnknownCollectionName               = errors.New("unknown collection name")
	ErrCollectionsNotEnabled               = errors.New("collections not enabled")
	ErrDocNotFound                         = errors.New("document not found")
	ErrDocExists                           = errors.New("document already exists")
	ErrDocNotStored                        = errors.New("document not stored")
	ErrValueTooLarge                       = errors.New("value too large")
	ErrAuthError                           = errors.New("auth error")
	ErrNotMyVbucket                        = errors.New("not my vbucket")
	ErrCasMismatch                         = errors.New("cas mismatch")
	ErrDocLocked                           = errors.New("document locked")
	ErrDocNotLocked                        = errors.New("document not locked")
	ErrDeltaBadval                         = errors.New("bad document value for delta operation")
	ErrAccessError                         = errors.New("access error")
	ErrRangeScanEmpty                      = errors.New("range scan range was empty")
	ErrRangeScanSeqNoNotFound              = errors.New("range scan sequence number not found")
	ErrRangeScanRangeError                 = errors.New("range scan range error")
	ErrRangeScanVbUuidMismatch             = errors.New("range scan vb-uuid mismatch")
	ErrRangeScanNotFound                   = errors.New("range scan uuid not found")
	ErrRangeScanCancelled                  = errors.New("range scan was cancelled")
	ErrSubDocPathNotFound                  = errors.New("subdoc path not found")
	ErrSubDocPathMismatch                  = errors.New("subdoc path mismatch")
	ErrSubDocPathInvalid                   = errors.New("subdoc patch invalid")
	ErrSubDocPathTooBig                    = errors.New("subdoc path too big")
	ErrSubDocDocTooDeep                    = errors.New("subdoc too deep")
	ErrSubDocCantInsert                    = errors.New("subdoc cant insert")
	ErrSubDocNotJSON                       = errors.New("subdoc not json")
	ErrSubDocBadRange                      = errors.New("subdoc bad range")
	ErrSubDocBadDelta                      = errors.New("subdoc bad delta")
	ErrSubDocPathExists                    = errors.New("subdoc path exists")
	ErrSubDocValueTooDeep                  = errors.New("subdoc value too deep")
	ErrSubDocInvalidCombo                  = errors.New("subdoc invalid combo")
	ErrSubDocXattrInvalidFlagCombo         = errors.New("subdoc xattr invalid flag combo")
	ErrSubDocXattrInvalidKeyCombo          = errors.New("subdoc xattr invalid key combo")
	ErrSubDocXattrUnknownMacro             = errors.New("subdoc xattr unknown macro")
	ErrSubDocXattrUnknownVAttr             = errors.New("subdoc xattr unknown vattr")
	ErrSubDocXattrCannotModifyVAttr        = errors.New("subdoc xattr cannot modify vattr")
	ErrSubDocInvalidXattrOrder             = errors.New("subdoc invalid xattr order")
	ErrSubDocXattrUnknownVattrMacro        = errors.New("subdoc xattr unknown vattr macro")
	ErrSubDocCanOnlyReviveDeletedDocuments = errors.New("subdoc can only revive deleted documents")
	ErrSubDocDeletedDocumentCantHaveValue  = errors.New("subdoc deleted document cant have value")
	ErrSyncWriteAmbiguous                  = errors.New("sync write was ambiguous")
	ErrSyncWriteInProgress                 = errors.New("sync write in progress")
	ErrSyncWriteReCommitInProgress         = errors.New("sync write recommit in progress")
	ErrTmpFail                             = errors.New("temporary failure")
	ErrDcpRollback                         = errors.New("dcp rollback")
	ErrDcpDuplicateStream                  = errors.New("duplicate dcp vbucket stream")

	ErrConfigNotSet   = errors.New("config not set")
	ErrClosedInFlight = errors.New("connection closed whilst operation in flight")
)

var ErrProtocol = errors.New("protocol error")

type protocolError struct {
	message string
}

func (e protocolError) Error() string {
	return "protocol error: " + e.message
}

func (e protocolError) Unwrap() error {
	return ErrProtocol
}

var ErrDispatch = errors.New("dispatch error")

type dispatchError struct {
	cause error
}

func (e dispatchError) Error() string {
	return "dispatch error: " + e.cause.Error()
}

func (e dispatchError) Unwrap() error {
	return ErrDispatch
}

type requestCancelledError struct {
	cause error
}

func (e requestCancelledError) Error() string {
	return "request cancelled: " + e.cause.Error()
}

func (e requestCancelledError) Unwrap() error {
	return e.cause
}

var ErrInvalidArgument = errors.New("invalid argument")

type invalidArgError struct {
	message string
}

func (e invalidArgError) Error() string {
	return "invalid argument error: " + e.message
}

func (e invalidArgError) Unwrap() error {
	return ErrInvalidArgument
}

type ServerError struct {
	Magic  Magic
	OpCode OpCode
	Status Status
	Cause  error
	Opaque uint32
}

func (e ServerError) Error() string {
	return fmt.Sprintf(
		"server error: %s, status: 0x%x, opcode: %s, opaque: %d",
		e.Cause,
		uint16(e.Status),
		e.OpCode.String(e.Magic),
		e.Opaque,
	)
}

func (e ServerError) Unwrap() error {
	return e.Cause
}

type SubDocError struct {
	Cause   error
	OpIndex int
}

func (e SubDocError) Error() string {
	return fmt.Sprintf(
		"subdoc operation error: %s (index: %d)",
		e.Cause,
		e.OpIndex,
	)
}

func (e SubDocError) Unwrap() error {
	return e.Cause
}

type ServerErrorWithConfig struct {
	Cause      ServerError
	ConfigJson []byte
}

func (e ServerErrorWithConfig) Error() string {
	return fmt.Sprintf("%s (config was attached)", e.Cause)
}

func (e ServerErrorWithConfig) Unwrap() error {
	return e.Cause
}

type ServerErrorContext struct {
	Text        string
	Ref         string
	ManifestRev uint64
}

type ServerErrorWithContext struct {
	Cause       ServerError
	ContextJson json.RawMessage
}

func (e ServerErrorWithContext) Error() string {
	return fmt.Sprintf("%s (context: `%s`)", e.Cause, e.ParseContext().Text)
}

func (e ServerErrorWithContext) Unwrap() error {
	return e.Cause
}

type serverErrorWithContextParsedContextJson struct {
	Context string `json:"context"`
}

type serverErrorWithContextParsedJson struct {
	Error       serverErrorWithContextParsedContextJson `json:"error"`
	Ref         string                                  `json:"ref"`
	ManifestUID string                                  `json:"manifest_uid"`
}

func (o ServerErrorWithContext) ParseContext() ServerErrorContext {
	var contextOut ServerErrorContext

	if len(o.ContextJson) == 0 {
		return contextOut
	}

	var parsedJson serverErrorWithContextParsedJson
	err := json.Unmarshal(o.ContextJson, &parsedJson)
	if err != nil {
		return contextOut
	}

	if parsedJson.Error.Context != "" {
		contextOut.Text = parsedJson.Error.Context
	}
	if parsedJson.Ref != "" {
		contextOut.Ref = parsedJson.Ref
	}
	if parsedJson.ManifestUID != "" {
		val, err := strconv.ParseUint(parsedJson.ManifestUID, 16, 64)
		if err == nil {
			contextOut.ManifestRev = val
		}
	}

	return contextOut
}

type ResourceError struct {
	Cause          error
	ScopeName      string
	CollectionName string
}

func (e ResourceError) Error() string {
	return fmt.Sprintf(
		"resource error: %s (scope: %s, collection: %s)",
		e.Cause,
		e.ScopeName,
		e.CollectionName)
}

func (e ResourceError) Unwrap() error {
	return e.Cause
}

type DcpRollbackError struct {
	RollbackSeqNo uint64
}

func (e DcpRollbackError) Error() string {
	return fmt.Sprintf(
		"%s (rollback seqno: %016x)",
		ErrDcpRollback,
		e.RollbackSeqNo)
}

func (e DcpRollbackError) Unwrap() error {
	return ErrDcpRollback
}
