package cbsearchx

import (
	"errors"
	"fmt"
)

var (
	ErrInternalServerError      = errors.New("internal server error")
	ErrAuthenticationFailure    = errors.New("auth error")
	ErrIndexNotFound            = errors.New("index not found")
	ErrIndexExists              = errors.New("index exists")
	ErrUnknownIndexType         = errors.New("unknown index type")
	ErrSourceTypeIncorrect      = errors.New("source type incorrect")
	ErrSourceNotFound           = errors.New("source not found")
	ErrNoIndexPartitionsPlanned = errors.New("no index partitions planned")
	ErrNoIndexPartitionsFound   = errors.New("no index partitions found")
	ErrUnsupportedFeature       = errors.New("unsupported feature")
)

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
	Cause      error
	StatusCode int
	Body       []byte
}

func (e ServerError) Error() string {
	return fmt.Sprintf("search query server error: %s (status: %d, body: `%s`)", e.Cause.Error(), e.StatusCode, e.Body)
}

func (e ServerError) Unwrap() error {
	return e.Cause
}

type SearchError struct {
	Cause      error
	StatusCode int
	Body       []byte
	Endpoint   string
}

func (e SearchError) Error() string {
	return fmt.Sprintf(
		"search query error: %s (status: %d, endpoint: `%s`, body: `%s`)",
		e.Cause.Error(),
		e.StatusCode,
		e.Endpoint,
		e.Body)
}

func (e SearchError) Unwrap() error {
	return e.Cause
}
