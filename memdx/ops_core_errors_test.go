package memdx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Testing private functions isn't ideal but by far the way to ensure that our error handling does
// what is expected for all cases.
func TestOpsCoreDecodeError(t *testing.T) {
	type test struct {
		Name          string
		Pkt           *Packet
		ExpectedError error
	}

	tests := []test{
		{
			Name: "NotMyVbucket",
			Pkt: &Packet{
				Magic:  MagicResExt,
				OpCode: OpCodeReplace,
				Status: StatusNotMyVBucket,
				Opaque: 0x34,
				Value:  []byte("impretendingtobeaconfig"),
			},
			ExpectedError: &ServerErrorWithConfig{
				Cause: ServerError{
					Magic:  MagicResExt,
					OpCode: OpCodeReplace,
					Status: StatusNotMyVBucket,
					Cause:  ErrNotMyVbucket,
					Opaque: 0x34,
				},
				ConfigJson: []byte("impretendingtobeaconfig"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			err := OpsCore{}.decodeError(test.Pkt)

			assert.Equal(t, test.ExpectedError, err)
		})
	}
}
