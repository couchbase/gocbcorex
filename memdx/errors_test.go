package memdx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// This is a very basic text which just tests that calling Error() parses the information
// out of the error value returned by the server.
func TestServerErrorWithContextText(t *testing.T) {
	text := []byte(`{"error":{"context":"Request must not include extras"}}`)

	baseCause := ServerError{
		Cause:          errors.New("invalid"),
		DispatchedTo:   "",
		DispatchedFrom: "",
		Opaque:         1,
	}
	err := ServerErrorWithContext{
		Cause:       baseCause,
		ContextJson: text,
	}

	assert.Contains(t, err.Error(), "Request must not include extras")
}
