package cbqueryx

import (
	"testing"
	"time"

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

	t.Run("scan_wait", func(t *testing.T) {
		opts := &QueryOptions{
			ScanWait: 2500 * time.Millisecond,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"scan_wait":"2.5s"}`, string(optsJson))
	})

	t.Run("timeout", func(t *testing.T) {
		opts := &QueryOptions{
			Timeout: 2500 * time.Millisecond,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"timeout":"2.5s"}`, string(optsJson))
	})

	t.Run("tx_timeout", func(t *testing.T) {
		opts := &QueryOptions{
			TxTimeout: 2500 * time.Millisecond,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"txtimeout":"2.5s"}`, string(optsJson))
	})

	t.Run("kvtimeout", func(t *testing.T) {
		opts := &QueryOptions{
			KvTimeout: 2500 * time.Millisecond,
		}

		optsJson, err := opts.encodeToJson()
		assert.NoError(t, err)

		assert.Equal(t, `{"kvtimeout":"2.5s"}`, string(optsJson))
	})
}
