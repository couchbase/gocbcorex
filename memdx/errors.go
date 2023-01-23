package memdx

import "errors"

var (
	ErrUnknownBucketName     = errors.New("unknown bucket name")
	ErrUnknownCollectionID   = errors.New("unknown collection id")
	ErrUnknownScopeName      = errors.New("unknown scope name")
	ErrUnknownCollectionName = errors.New("unknown collection name")
	ErrCollectionsNotEnabled = errors.New("collections not enabled")
	ErrDocNotFound           = errors.New("document not found")
	ErrAuthError             = errors.New("auth error")
	ErrNotMyVbucket          = errors.New("not my vbucket")
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
