package cbhttpx

import (
	"errors"
	"fmt"
)

var (
	ErrConnectError = errors.New("http connect failed")
)

type ConnectError struct {
	Cause error
}

func (e ConnectError) Error() string {
	return fmt.Sprintf("%s: %s", ErrConnectError, e.Cause)
}

func (e ConnectError) Unwrap() error {
	return ErrConnectError
}
