package cbauthx

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidAuth     = errors.New("invalid auth")
	ErrClosed          = errors.New("already closed")
	ErrLivenessTimeout = errors.New("cache is stale")
)

type contextualError struct {
	Message string
	Cause   error
}

func (e contextualError) Error() string {
	if e.Cause == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Message, e.Cause.Error())
}

func (e contextualError) Unwrap() error {
	return e.Cause
}

type ServerError struct {
	StatusCode int
	Body       []byte
}

func (e ServerError) Error() string {
	return fmt.Sprintf("server error (status: %d, body: `%s`)", e.StatusCode, e.Body)
}

type CacheStaleError struct {
	Reason error
}

func (e CacheStaleError) Error() string {
	reasonStr := "*nil*"
	if e.Reason != nil {
		reasonStr = e.Reason.Error()
	}
	return fmt.Sprintf("cache is stale (reason: %s)", reasonStr)
}

func (e CacheStaleError) Unwrap() error {
	return ErrLivenessTimeout
}

type MultiConnectError struct {
	Reasons []error
}

func (e MultiConnectError) Error() string {
	reasons := make([]string, 0, len(e.Reasons))
	for _, err := range e.Reasons {
		if err != nil {
			reasons = append(reasons, err.Error())
		} else {
			reasons = append(reasons, "*nil*")
		}
	}
	return fmt.Sprintf("failed to connect to all hosts: %s",
		strings.Join(reasons, ", "))
}
