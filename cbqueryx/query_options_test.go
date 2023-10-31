package cbqueryx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeQueryOptions(t *testing.T) {
	opts := &Options{
		Statement: "SELECT *",
	}

	optsJson, err := opts.encodeToJson()
	assert.NoError(t, err)

	assert.Equal(t, `{"statement":"SELECT *"}`, string(optsJson))
}
