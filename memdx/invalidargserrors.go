package memdx

import (
	"errors"
	"strings"
)

type InvalidArgsErrorType int

const (
	InvalidArgsErrorUnknown = InvalidArgsErrorType(iota)
	InvalidArgsErrorCannotInflate
)

func ParseInvalidArgsError(err error) InvalidArgsErrorType {
	var errWithContext *ServerErrorWithContext
	if !errors.As(err, &errWithContext) {
		return InvalidArgsErrorUnknown
	}

	parsedContext := errWithContext.ParseContext()
	errText := strings.ToLower(parsedContext.Text)

	if strings.Contains(errText, "failed to inflate payload") {
		return InvalidArgsErrorCannotInflate
	}

	return InvalidArgsErrorUnknown
}
