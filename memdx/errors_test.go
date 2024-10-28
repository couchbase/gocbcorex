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

	err := &ServerErrorWithContext{
		Cause: ServerError{
			Cause:  errors.New("invalid"),
			Opaque: 1,
		},
		ContextJson: text,
	}

	assert.Contains(t, err.Error(), "Request must not include extras")
}
