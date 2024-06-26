package transactionsx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrNoAttempt indicates no attempt was started before an operation was performed.
	ErrNoAttempt = errors.New("attempt was not started")

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = errors.New("atr full")

	// ErrAttemptExpired indicates an attempt expired.
	ErrAttemptExpired = errors.New("attempt expired")

	// ErrAtrNotFound indicates that an expected ATR document was missing.
	ErrAtrNotFound = errors.New("atr not found")

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing.
	ErrAtrEntryNotFound = errors.New("atr entry not found")

	// ErrIllegalState is used for when a transaction enters an illegal State.
	ErrIllegalState = errors.New("illegal State")

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrTransactionAbortedExternally = errors.New("transaction aborted externally")

	// ErrPreviousOperationFailed indicates a previous operation in the transaction failed.
	ErrPreviousOperationFailed = errors.New("previous operation failed")

	// ErrForwardCompatibilityFailure indicates an operation failed due to involving a document in another transaction
	// which contains features this transaction does not support.
	ErrForwardCompatibilityFailure = errors.New("forward compatibility error")

	// ErrDocNotFound indicates that a document was not found.
	ErrDocNotFound = errors.New("document not found (txns)")

	// ErrDocExists indicates that a document already exists.
	ErrDocExists = errors.New("document exists (txns)")
)

type classifiedError struct {
	Source error
	Class  TransactionErrorClass
}

func (ce classifiedError) Wrap(errType error) *classifiedError {
	return &classifiedError{
		Source: &basicRetypedError{
			ErrType: errType,
			Source:  ce.Source,
		},
		Class: ce.Class,
	}
}

type TransactionAttemptError struct {
	Cause  error
	Result *TransactionAttemptResult
}

func (e *TransactionAttemptError) Error() string {
	return fmt.Sprintf("transaction attempt error (result: %+v): %s",
		e.Result, e.Cause)
}

func (e *TransactionAttemptError) Unwrap() error {
	return e.Cause
}

type TransactionPostErrorRollbackError struct {
	OriginalCause error
	RollbackErr   error
}

func (e *TransactionPostErrorRollbackError) Error() string {
	return fmt.Sprintf("post-failure rollback error (rollback error: %s): %s",
		e.RollbackErr, e.OriginalCause)
}

func (e *TransactionPostErrorRollbackError) Unwrap() error {
	return e.OriginalCause
}

type TransactionOperationError struct {
	ShouldNotRetry bool
	Cause          error
	ShouldRaise    TransactionErrorReason
	ErrorClass     TransactionErrorClass
	Result         *TransactionAttemptResult
}

func (e *TransactionOperationError) Error() string {
	return fmt.Sprintf("transaction operation error (shouldNotRetry: %t, shouldRaise: %d, errorClass: %d, result: ): %s",
		e.ShouldNotRetry, e.ShouldRaise, e.ErrorClass, e.Cause)
}

func (e *TransactionOperationError) Unwrap() error {
	return e.Cause
}

// transactionOperationStatus is used when a transaction operation fails.
type transactionOperationStatus struct {
	canStillCommit    bool
	shouldNotRetry    bool
	shouldNotRollback bool
	errorCause        error
	shouldRaise       TransactionErrorReason
	errorClass        TransactionErrorClass
}

func (s *transactionOperationStatus) Err() error {
	if s.shouldRaise == TransactionErrorReasonSuccess {
		// if the transaction error reason is still success, this means that the operation
		// has not been fatal to the transaction, so we can just return the cause itself.
		return s.errorCause
	}

	return &TransactionOperationError{
		ShouldNotRetry: s.shouldNotRetry,
		Cause:          s.errorCause,
		ShouldRaise:    s.shouldRaise,
		ErrorClass:     s.errorClass,
	}
}

type aggregateError []error

func (agge aggregateError) MarshalJSON() ([]byte, error) {
	suberrs := make([]json.RawMessage, len(agge))
	for i, err := range agge {
		suberrs[i] = marshalErrorToJSON(err)
	}
	return json.Marshal(suberrs)
}

func (agge aggregateError) Error() string {
	errStrs := []string{}
	for _, err := range agge {
		errStrs = append(errStrs, err.Error())
	}
	return "[" + strings.Join(errStrs, ", ") + "]"
}

func (agge aggregateError) Is(err error) bool {
	for _, aerr := range agge {
		if errors.Is(aerr, err) {
			return true
		}
	}
	return false
}

type writeWriteConflictError struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocumentKey    []byte
	Source         error
}

func (wwce writeWriteConflictError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Msg            string          `json:"msg"`
		Cause          json.RawMessage `json:"cause"`
		BucketName     string          `json:"bucket"`
		ScopeName      string          `json:"scope"`
		CollectionName string          `json:"collection"`
		DocumentKey    string          `json:"document_key"`
	}{
		Msg:            "write write conflict",
		Cause:          marshalErrorToJSON(wwce.Source),
		BucketName:     wwce.BucketName,
		ScopeName:      wwce.ScopeName,
		CollectionName: wwce.CollectionName,
		DocumentKey:    string(wwce.DocumentKey),
	})
}

func (wwce writeWriteConflictError) Error() string {
	errStr := "write write conflict"
	errStr += " | " + fmt.Sprintf(
		"bucket:%s, scope:%s, collection:%s, key:%s",
		wwce.BucketName,
		wwce.ScopeName,
		wwce.CollectionName,
		wwce.DocumentKey)
	if wwce.Source != nil {
		errStr += " | " + wwce.Source.Error()
	}
	return errStr
}

func (wwce writeWriteConflictError) Unwrap() error {
	return wwce.Source
}

type basicRetypedError struct {
	ErrType error
	Source  error
}

func (bre basicRetypedError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Msg   string          `json:"msg"`
		Cause json.RawMessage `json:"cause"`
	}{
		Msg:   bre.ErrType.Error(),
		Cause: marshalErrorToJSON(bre.Source),
	})
}

func (bre basicRetypedError) Error() string {
	errStr := bre.ErrType.Error()
	if bre.Source != nil {
		errStr += " | " + bre.Source.Error()
	}
	return errStr
}

func (bre basicRetypedError) Is(err error) bool {
	if errors.Is(bre.ErrType, err) {
		return true
	}
	return errors.Is(bre.Source, err)
}

func (bre basicRetypedError) Unwrap() error {
	return bre.Source
}

type forwardCompatError struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocumentKey    []byte
}

func (fce forwardCompatError) Error() string {
	errStr := ErrForwardCompatibilityFailure.Error()
	errStr += " | " + fmt.Sprintf(
		"bucket:%s, scope:%s, collection:%s, key:%s",
		fce.BucketName,
		fce.ScopeName,
		fce.CollectionName,
		fce.DocumentKey)
	return errStr
}

func (fce forwardCompatError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		BucketName     string `json:"bucket,omitempty"`
		ScopeName      string `json:"scope,omitempty"`
		CollectionName string `json:"collection,omitempty"`
		DocumentKey    string `json:"document_key,omitempty"`
		Message        string `json:"msg"`
	}{
		BucketName:     fce.BucketName,
		ScopeName:      fce.ScopeName,
		CollectionName: fce.CollectionName,
		DocumentKey:    string(fce.DocumentKey),
		Message:        ErrForwardCompatibilityFailure.Error(),
	})
}

func (fce forwardCompatError) Unwrap() error {
	return ErrForwardCompatibilityFailure
}

func marshalErrorToJSON(err error) json.RawMessage {
	if marshaler, ok := err.(json.Marshaler); ok {
		if data, err := marshaler.MarshalJSON(); err == nil {
			return data
		}
	}

	// Marshalling a string cannot fail
	data, _ := json.Marshal(err.Error())
	return data
}

type ErrorClassError struct {
	Class TransactionErrorClass
}

func (e ErrorClassError) Error() string {
	return fmt.Sprintf("error class error - %d", e.Class)
}

type wrappedError struct {
	Message    string
	InnerError error
}

func (e wrappedError) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.InnerError.Error())
}

func (e wrappedError) Unwrap() error {
	return e.InnerError
}

func wrapError(err error, message string) error {
	return wrappedError{
		Message:    message,
		InnerError: err,
	}
}
