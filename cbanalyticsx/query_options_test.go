package cbanalyticsx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeQueryOptions(t *testing.T) {
	t.Run("statement", func(t *testing.T) {
		opts := &QueryOptions{
			Statement: "SELECT *",
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"statement":"SELECT *"}`, string(optsJson))
	})

	t.Run("scan_consistency", func(t *testing.T) {
		opts := &QueryOptions{
			ScanConsistency: ScanConsistencyRequestPlus,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"scan_consistency":"request_plus"}`, string(optsJson))
	})

	t.Run("priority", func(t *testing.T) {
		// priority is a header, so make sure we don't encode it
		opts := &QueryOptions{
			Priority: -2,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{}`, string(optsJson))
	})
}
