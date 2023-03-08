package memdx

import "encoding/hex"

type Status uint16

const (
	// StatusSuccess indicates the operation completed successfully.
	StatusSuccess = Status(0x00)

	// StatusKeyNotFound occurs when an operation is performed on a key that does not exist.
	StatusKeyNotFound = Status(0x01)

	// StatusKeyExists occurs when an operation is performed on a key that could not be found.
	StatusKeyExists = Status(0x02)

	// StatusTooBig occurs when an operation attempts to store more data in a single document
	// than the server is capable of storing (by default, this is a 20MB limit).
	StatusTooBig = Status(0x03)

	// StatusInvalidArgs occurs when the server receives invalid arguments for an operation.
	StatusInvalidArgs = Status(0x04)

	// StatusNotStored occurs when the server fails to store a key.
	StatusNotStored = Status(0x05)

	// StatusBadDelta occurs when an invalid delta value is specified to a counter operation.
	StatusBadDelta = Status(0x06)

	// StatusNotMyVBucket occurs when an operation is dispatched to a server which is
	// non-authoritative for a specific vbucket.
	StatusNotMyVBucket = Status(0x07)

	// StatusNoBucket occurs when no bucket was selected on a connection.
	StatusNoBucket = Status(0x08)

	// StatusLocked occurs when an operation fails due to the document being locked.
	StatusLocked = Status(0x09)

	// StatusAuthStale occurs when authentication credentials have become invalidated.
	StatusAuthStale = Status(0x1f)

	// StatusAuthError occurs when the authentication information provided was not valid.
	StatusAuthError = Status(0x20)

	// StatusAuthContinue occurs in multi-step authentication when more authentication
	// work needs to be performed in order to complete the authentication process.
	StatusAuthContinue = Status(0x21)

	// StatusRangeError occurs when the range specified to the server is not valid.
	StatusRangeError = Status(0x22)

	// StatusRollback occurs when a DCP stream fails to open due to a rollback having
	// previously occurred since the last time the stream was opened.
	StatusRollback = Status(0x23)

	// StatusAccessError occurs when an access error occurs.
	StatusAccessError = Status(0x24)

	// StatusNotInitialized is sent by servers which are still initializing, and are not
	// yet ready to accept operations on behalf of a particular bucket.
	StatusNotInitialized = Status(0x25)

	// StatusRateLimitedNetworkIngress occurs when the server rate limits due to network ingress.
	StatusRateLimitedNetworkIngress = Status(0x30)

	// StatusRateLimitedNetworkEgress occurs when the server rate limits due to network egress.
	StatusRateLimitedNetworkEgress = Status(0x31)

	// StatusRateLimitedMaxConnections occurs when the server rate limits due to the application reaching the maximum
	// number of allowed connections.
	StatusRateLimitedMaxConnections = Status(0x32)

	// StatusRateLimitedMaxCommands occurs when the server rate limits due to the application reaching the maximum
	// number of allowed operations.
	StatusRateLimitedMaxCommands = Status(0x33)

	// StatusRateLimitedScopeSizeLimitExceeded occurs when the server rate limits due to the application reaching the maximum
	// data size allowed for the scope.
	StatusRateLimitedScopeSizeLimitExceeded = Status(0x34)

	// StatusUnknownCommand occurs when an unknown operation is sent to a server.
	StatusUnknownCommand = Status(0x81)

	// StatusOutOfMemory occurs when the server cannot service a request due to memory
	// limitations.
	StatusOutOfMemory = Status(0x82)

	// StatusNotSupported occurs when an operation is understood by the server, but that
	// operation is not supported on this server (occurs for a variety of reasons).
	StatusNotSupported = Status(0x83)

	// StatusInternalError occurs when internal errors prevent the server from processing
	// your request.
	StatusInternalError = Status(0x84)

	// StatusBusy occurs when the server is too busy to process your request right away.
	// Attempting the operation at a later time will likely succeed.
	StatusBusy = Status(0x85)

	// StatusTmpFail occurs when a temporary failure is preventing the server from
	// processing your request.
	StatusTmpFail = Status(0x86)

	// StatusCollectionUnknown occurs when a Collection cannot be found.
	StatusCollectionUnknown = Status(0x88)

	// StatusScopeUnknown occurs when a Scope cannot be found.
	StatusScopeUnknown = Status(0x8c)

	// StatusDCPStreamIDInvalid occurs when a dcp stream ID is invalid.
	StatusDCPStreamIDInvalid = Status(0x8d)

	// StatusDurabilityInvalidLevel occurs when an invalid durability level was requested.
	StatusDurabilityInvalidLevel = Status(0xa0)

	// StatusDurabilityImpossible occurs when a request is performed with impossible
	// durability level requirements.
	StatusDurabilityImpossible = Status(0xa1)

	// StatusSyncWriteInProgress occurs when an attempt is made to write to a key that has
	// a SyncWrite pending.
	StatusSyncWriteInProgress = Status(0xa2)

	// StatusSyncWriteAmbiguous occurs when an SyncWrite does not complete in the specified
	// time and the result is ambiguous.
	StatusSyncWriteAmbiguous = Status(0xa3)

	// StatusSyncWriteReCommitInProgress occurs when an SyncWrite is being recommitted.
	StatusSyncWriteReCommitInProgress = Status(0xa4)

	// StatusRangeScanCancelled occurs during a range scan to indicate that the range scan was cancelled.
	StatusRangeScanCancelled = Status(0xa5)

	// StatusRangeScanMore occurs during a range scan to indicate that a range scan has more results.
	StatusRangeScanMore = Status(0xa6)

	// StatusRangeScanComplete occurs during a range scan to indicate that a range scan has completed.
	StatusRangeScanComplete = Status(0xa7)

	// StatusRangeScanVbUUIDNotEqual occurs during a range scan to indicate that a vb-uuid mismatch has occurred.
	StatusRangeScanVbUUIDNotEqual = Status(0xa8)

	// StatusSubDocPathNotFound occurs when a sub-document operation targets a path
	// which does not exist in the specifie document.
	StatusSubDocPathNotFound = Status(0xc0)

	// StatusSubDocPathMismatch occurs when a sub-document operation specifies a path
	// which does not match the document structure (field access on an array).
	StatusSubDocPathMismatch = Status(0xc1)

	// StatusSubDocPathInvalid occurs when a sub-document path could not be parsed.
	StatusSubDocPathInvalid = Status(0xc2)

	// StatusSubDocPathTooBig occurs when a sub-document path is too big.
	StatusSubDocPathTooBig = Status(0xc3)

	// StatusSubDocDocTooDeep occurs when an operation would cause a document to be
	// nested beyond the depth limits allowed by the sub-document specification.
	StatusSubDocDocTooDeep = Status(0xc4)

	// StatusSubDocCantInsert occurs when a sub-document operation could not insert.
	StatusSubDocCantInsert = Status(0xc5)

	// StatusSubDocNotJSON occurs when a sub-document operation is performed on a
	// document which is not JSON.
	StatusSubDocNotJSON = Status(0xc6)

	// StatusSubDocBadRange occurs when a sub-document operation is performed with
	// a bad range.
	StatusSubDocBadRange = Status(0xc7)

	// StatusSubDocBadDelta occurs when a sub-document counter operation is performed
	// and the specified delta is not valid.
	StatusSubDocBadDelta = Status(0xc8)

	// StatusSubDocPathExists occurs when a sub-document operation expects a path not
	// to exists, but the path was found in the document.
	StatusSubDocPathExists = Status(0xc9)

	// StatusSubDocValueTooDeep occurs when a sub-document operation specifies a value
	// which is deeper than the depth limits of the sub-document specification.
	StatusSubDocValueTooDeep = Status(0xca)

	// StatusSubDocInvalidCombo occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	StatusSubDocInvalidCombo = Status(0xcb)

	// StatusSubDocMultiPathFailure occurs when a multi-operation sub-document operation is
	// performed and operations within the package of ops conflict with each other.
	StatusSubDocMultiPathFailure = Status(0xcc)

	// StatusSubDocSuccessDeleted occurs when a multi-operation sub-document operation
	// is performed on a soft-deleted document.
	StatusSubDocSuccessDeleted = Status(0xcd)

	// StatusSubDocXattrInvalidFlagCombo occurs when an invalid set of
	// extended-attribute flags is passed to a sub-document operation.
	StatusSubDocXattrInvalidFlagCombo = Status(0xce)

	// StatusSubDocXattrInvalidKeyCombo occurs when an invalid set of key operations
	// are specified for a extended-attribute sub-document operation.
	StatusSubDocXattrInvalidKeyCombo = Status(0xcf)

	// StatusSubDocXattrUnknownMacro occurs when an invalid macro value is specified.
	StatusSubDocXattrUnknownMacro = Status(0xd0)

	// StatusSubDocXattrUnknownVAttr occurs when an invalid virtual attribute is specified.
	StatusSubDocXattrUnknownVAttr = Status(0xd1)

	// StatusSubDocXattrCannotModifyVAttr occurs when a mutation is attempted upon
	// a virtual attribute (which are immutable by definition).
	StatusSubDocXattrCannotModifyVAttr = Status(0xd2)

	// StatusSubDocMultiPathFailureDeleted occurs when a Multi Path Failure occurs on
	// a soft-deleted document.
	StatusSubDocMultiPathFailureDeleted = Status(0xd3)

	// StatusSubDocInvalidXattrOrder occurs when xattr operations exist after non-xattr
	// operations in the operation list.
	StatusSubDocInvalidXattrOrder = Status(0xd4)

	// StatusSubDocXattrUnknownVattrMacro occurs when you try to use an unknown vattr.
	StatusSubDocXattrUnknownVattrMacro = Status(0xd5)

	// StatusSubDocCanOnlyReviveDeletedDocuments occurs when you try to revive a document
	// which is not currently in the soft-deleted state.
	StatusSubDocCanOnlyReviveDeletedDocuments = Status(0xd6)

	// StatusSubDocDeletedDocumentCantHaveValue occurs when you try set a value to a
	// soft-deleted document.
	StatusSubDocDeletedDocumentCantHaveValue = Status(0xd7)
)

// String returns the textual representation of this Status.
func (s Status) String() string {
	switch s {
	case StatusSuccess:
		return "Success"
	case StatusKeyNotFound:
		return "KeyNotFound"
	case StatusKeyExists:
		return "KeyExists"
	case StatusTooBig:
		return "TooBig"
	case StatusInvalidArgs:
		return "InvalidArgs"
	case StatusNotStored:
		return "NotStored"
	case StatusBadDelta:
		return "BadDelta"
	case StatusNotMyVBucket:
		return "NotMyVBucket"
	case StatusNoBucket:
		return "NoBucket"
	case StatusAuthStale:
		return "AuthStale"
	case StatusAuthError:
		return "AuthError"
	case StatusAuthContinue:
		return "AuthContinue"
	case StatusRangeError:
		return "RangeError"
	case StatusAccessError:
		return "AccessError"
	case StatusNotInitialized:
		return "NotInitialized"
	case StatusRollback:
		return "Rollback"
	case StatusUnknownCommand:
		return "UnknownCommand"
	case StatusOutOfMemory:
		return "OutOfMemory"
	case StatusNotSupported:
		return "NotSupported"
	case StatusInternalError:
		return "InternalError"
	case StatusBusy:
		return "Busy"
	case StatusTmpFail:
		return "TmpFail"
	case StatusCollectionUnknown:
		return "CollectionUnknown"
	case StatusScopeUnknown:
		return "ScopeUnknown"
	case StatusDCPStreamIDInvalid:
		return "DCPStreamIDInvalid"
	case StatusDurabilityInvalidLevel:
		return "DurabilityInvalidLevel"
	case StatusDurabilityImpossible:
		return "DurabilityImpossible"
	case StatusSyncWriteInProgress:
		return "SyncWriteInProgress"
	case StatusSyncWriteAmbiguous:
		return "SyncWriteAmbiguous"
	case StatusSubDocPathNotFound:
		return "SubDocPathNotFound"
	case StatusSubDocPathMismatch:
		return "SubDocPathMismatch"
	case StatusSubDocPathInvalid:
		return "SubDocPathInvalid"
	case StatusSubDocPathTooBig:
		return "SubDocPathTooBig"
	case StatusSubDocDocTooDeep:
		return "SubDocDocTooDeep"
	case StatusSubDocCantInsert:
		return "SubDocCantInsert"
	case StatusSubDocNotJSON:
		return "SubDocNotJSON"
	case StatusSubDocBadRange:
		return "SubDocBadRange"
	case StatusSubDocBadDelta:
		return "SubDocBadDelta"
	case StatusSubDocPathExists:
		return "SubDocPathExists"
	case StatusSubDocValueTooDeep:
		return "SubDocValueTooDeep"
	case StatusSubDocInvalidCombo:
		return "SubDocBadCombo"
	case StatusSubDocMultiPathFailure:
		return "SubDocBadMulti"
	case StatusSubDocSuccessDeleted:
		return "SubDocSuccessDeleted"
	case StatusSubDocXattrInvalidFlagCombo:
		return "SubDocXattrInvalidFlagCombo"
	case StatusSubDocXattrInvalidKeyCombo:
		return "SubDocXattrInvalidKeyCombo"
	case StatusSubDocXattrUnknownMacro:
		return "SubDocXattrUnknownMacro"
	case StatusSubDocXattrUnknownVAttr:
		return "SubDocXattrUnknownVAttr"
	case StatusSubDocXattrCannotModifyVAttr:
		return "SubDocXattrCannotModifyVAttr"
	case StatusSubDocMultiPathFailureDeleted:
		return "SubDocMultiPathFailureDeleted"
	case StatusSubDocXattrUnknownVattrMacro:
		return "SubdocXattrUnknownVattrMacro"
	case StatusSubDocCanOnlyReviveDeletedDocuments:
		return "SubDocCanOnlyReviveDeletedDocuments"
	case StatusSubDocDeletedDocumentCantHaveValue:
		return "SubDocDeletedDocumentCantHaveValue"
	case StatusSubDocInvalidXattrOrder:
		return "SubDocInvalidXattrOrder"
	}

	return "x" + hex.EncodeToString([]byte{byte(s)})
}
