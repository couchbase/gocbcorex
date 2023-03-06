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
	ErrAuthError                           = errors.New("auth error")
	ErrNotMyVbucket                        = errors.New("not my vbucket")
	ErrCasMismatch                         = errors.New("cas mismatch")
	ErrDocLocked                           = errors.New("document locked")
	ErrRangeEmpty                          = errors.New("requested range is empty")
	ErrSeqNoNotFound                       = errors.New("sequence number not found")
	ErrSampleRangeImpossible               = errors.New("not enough keys to satify requested sample scan")
	ErrVbUUIDMismatch                      = errors.New("vb-uuid mismatch")
	ErrScanNotFound                        = errors.New("scan uuid not found")
	ErrRangeScanCancelled                  = errors.New("range scan was cancelled")
	ErrAccessError                         = errors.New("access error")
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
	Cause          error
	DispatchedTo   string
	DispatchedFrom string
	Opaque         uint32
}

func (e ServerError) Error() string {
	return fmt.Sprintf(
		"server error: %s, dispatched from: %s, dispatched to: %s, opaque: %d",
		e.Cause,
		e.DispatchedFrom,
		e.DispatchedTo,
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
	return fmt.Sprintf("%s (context: `%s`)", e.Cause, e.ContextJson)
}

func (e ServerErrorWithContext) Unwrap() error {
	return e.Cause
}

func (o ServerErrorWithContext) ParseContext() ServerErrorContext {
	var contextOut ServerErrorContext

	if len(o.ContextJson) == 0 {
		return contextOut
	}

	parsedJson := struct {
		Context     string `json:"context"`
		Ref         string `json:"ref"`
		ManifestUID string `json:"manifest_uid"`
	}{}

	err := json.Unmarshal(o.ContextJson, &parsedJson)
	if err != nil {
		return contextOut
	}

	if parsedJson.Context != "" {
		contextOut.Text = parsedJson.Context
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
