package transactionsx

import "fmt"

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
