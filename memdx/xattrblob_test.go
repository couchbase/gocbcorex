package memdx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

/*
DOC: {"key":"value"}
_bar: {"y":"z"}
_foo: {"x":"y"}
*/
var TEST_VALUE []byte = []byte{
	0, 0, 0, 38, 0, 0, 0, 15, 95, 98, 97, 114, 0, 123, 34, 121,
	34, 58, 34, 122, 34, 125, 0, 0, 0, 0, 15, 95, 102, 111, 111, 0,
	123, 34, 120, 34, 58, 34, 121, 34, 125, 0, 123, 34, 107, 101, 121, 34,
	58, 34, 118, 97, 108, 117, 101, 34, 125}

func TestXattrBlob(t *testing.T) {
	xattrBlob, docValue, err := SplitXattrBlob(TEST_VALUE)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"key":"value"}`), docValue)

	remainingBytes := xattrBlob
	firstName, firstValue, n, err := DecodeXattrBlobEntry(remainingBytes)
	require.NoError(t, err)
	require.Equal(t, "_bar", firstName)
	require.Equal(t, `{"y":"z"}`, firstValue)

	remainingBytes = remainingBytes[n:]
	secondName, secondValue, n, err := DecodeXattrBlobEntry(remainingBytes)
	require.NoError(t, err)
	require.Equal(t, "_foo", secondName)
	require.Equal(t, `{"x":"y"}`, secondValue)

	remainingBytes = remainingBytes[n:]
	require.Empty(t, remainingBytes)

	newBlob := make([]byte, 0)
	newBlob = AppendXattrBlobEntry(newBlob, "_bar", `{"y":"z"}`)
	newBlob = AppendXattrBlobEntry(newBlob, "_foo", `{"x":"y"}`)

	newBytes := JoinXattrBlob(newBlob, docValue)
	require.Equal(t, TEST_VALUE, newBytes)
}
