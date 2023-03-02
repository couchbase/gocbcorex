package cbmgmtx

import (
	"errors"
	"fmt"
)

var (
	ErrAccessDenied       = errors.New("access denied")
	ErrUnsupportedFeature = errors.New("unsupported feature")
)

type ServerError struct {
	Cause      error
	StatusCode int
}

func (e ServerError) Error() string {
	return fmt.Sprintf("server error: %s (status: %d)", e.Cause.Error(), e.StatusCode)
}

func (e ServerError) Unwrap() error {
	return e.Cause
}
