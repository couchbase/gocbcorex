package memdx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseXattrCas(t *testing.T) {
	cas, err := ParseMacroCasToCas([]byte("0x000058a71dd25c15"))
	require.NoError(t, err)

	tm, err := ParseCasToTime(cas)
	require.NoError(t, err)
	require.Equal(t, time.Unix(0, 1539336197457313792), tm)
}
