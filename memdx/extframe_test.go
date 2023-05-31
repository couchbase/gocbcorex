package memdx

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDurabilityExtFrame(t *testing.T) {
	testOne := func(l DurabilityLevel, d time.Duration, expectedBytes []byte) error {
		data, err := EncodeDurabilityExtFrame(l, d)
		if err != nil {
			return err
		}

		assert.Equal(t, expectedBytes, data)

		lOut, dOut, err := DecodeDurabilityExtFrame(data)
		if err != nil {
			return err
		}

		assert.Equal(t, l, lOut)

		if d == 0 {
			assert.Equal(t,
				int64(d/time.Millisecond),
				int64(dOut/time.Millisecond))
		} else {
			assert.LessOrEqual(t,
				math.Abs(float64(dOut/time.Millisecond)-
					float64(d/time.Millisecond)),
				float64(1),
				"Expected relative difference less than 1ms")
		}

		return nil
	}

	assert.NoError(t, testOne(DurabilityLevelMajority, 0*time.Millisecond, []byte{0x01}))
	assert.NoError(t, testOne(DurabilityLevelMajorityAndPersistToActive, 0*time.Millisecond, []byte{0x02}))
	assert.NoError(t, testOne(DurabilityLevelMajority, 1*time.Nanosecond, []byte{0x01, 0x00, 0x01}))
	assert.NoError(t, testOne(DurabilityLevelMajority, 1*time.Millisecond, []byte{0x01, 0x00, 0x01}))
	assert.NoError(t, testOne(DurabilityLevelMajority, 12201*time.Millisecond, []byte{0x01, 0x2f, 0xa9}))
	assert.NoError(t, testOne(DurabilityLevelMajority, 65535*time.Millisecond, []byte{0x01, 0xff, 0xff}))
}

func TestServerDurationExtFrame(t *testing.T) {
	testOne := func(d time.Duration, expectedBytes []byte) error {
		data, err := EncodeServerDurationExtFrame(d)
		if err != nil {
			return err
		}

		assert.Equal(t, expectedBytes, data)

		dOut, err := DecodeServerDurationExtFrame(data)
		if err != nil {
			return err
		}

		// We compare these values in microseconds, as that is the 'resolution' of
		// the math that is used for transmission in the protocol.  We also allow
		// up to 1 millisecond of variance in the output values to allow fp errors.
		if d == 0 {
			assert.Equal(t,
				int64(d/time.Microsecond),
				int64(dOut/time.Microsecond))
		} else {
			assert.LessOrEqual(t,
				math.Abs(float64(dOut/time.Microsecond)-
					float64(d/time.Microsecond)),
				float64(1),
				"Expected relative difference less than 1us")
		}
		return nil
	}

	assert.NoError(t, testOne(0*time.Microsecond, []byte{0x00, 0x00}))
	assert.NoError(t, testOne(1*time.Microsecond, []byte{0x00, 0x01}))
	assert.NoError(t, testOne(9919*time.Microsecond, []byte{0x01, 0x27}))
	assert.NoError(t, testOne(89997489*time.Microsecond, []byte{0xd8, 0xda}))
	assert.NoError(t, testOne(99999149*time.Microsecond, []byte{0xe6, 0x64}))
	assert.NoError(t, testOne(109999659*time.Microsecond, []byte{0xf3, 0x5d}))
	assert.NoError(t, testOne(120125043*time.Microsecond, []byte{0xff, 0xff}))
}
