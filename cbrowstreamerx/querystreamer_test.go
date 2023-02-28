package cbrowstreamerx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testReadCloser struct {
	CloseErr  error
	ReadErr   error
	ReadValue []byte

	readPos int
}

func (rc *testReadCloser) Read(in []byte) (int, error) {
	if rc.ReadErr != nil {
		return 0, rc.ReadErr
	}

	val := rc.ReadValue[rc.readPos:]
	if len(in) >= len(val) {
		copy(in, val)
		rc.readPos = len(rc.ReadValue)
	} else {
		copy(in, val[:len(in)])
		rc.readPos += len(in)
	}

	return len(in), rc.ReadErr
}

func (rc *testReadCloser) Close() error {
	return rc.CloseErr
}

func TestQueryStreamer(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"rows":[{"test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.NoError(t, err)

	row := streamer.NextRow()
	assert.Equal(t, []byte(`{"test":"value"}`), row)

	row = streamer.NextRow()
	assert.Equal(t, []byte(`{"test2":"value2"}`), row)

	row = streamer.NextRow()
	assert.Nil(t, row)

	meta, err := streamer.MetaData()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"meta":{"testkey":"testvalue"}}`), meta)

	err = streamer.Err()
	require.NoError(t, err)

	err = streamer.Close()
	require.NoError(t, err)
}

func TestQueryStreamerOne(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"rows":[{"test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.NoError(t, err)

	row, err := streamer.One()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"test":"value"}`), row)

	row = streamer.NextRow()
	assert.Nil(t, row)

	meta, err := streamer.MetaData()
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"meta":{"testkey":"testvalue"}}`), meta)

	err = streamer.Err()
	require.NoError(t, err)

	err = streamer.Close()
	require.NoError(t, err)
}

func TestQueryStreamerInvalidFirstToken(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	_, err = NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`["rows":[{"test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.Error(t, err)
}

func TestQueryStreamerInvalidRowsFirstToken(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	_, err = NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"rows":{{"test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.Error(t, err)
}

func TestQueryStreamerNoRows(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.NoError(t, err)

	row := streamer.NextRow()
	assert.Nil(t, row)

	err = streamer.Err()
	require.NoError(t, err)

	err = streamer.Close()
	require.NoError(t, err)
}

func TestQueryStreamerOneNoRows(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.NoError(t, err)

	_, err = streamer.One()
	require.Error(t, err)
}

func TestQueryStreamerInvalidRow(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"rows":[test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
	}, "rows", logger)
	require.NoError(t, err)

	row := streamer.NextRow()
	assert.Nil(t, row)

	err = streamer.Err()
	require.Error(t, err)

	err = streamer.Close()
	require.Error(t, err)
}

func TestQueryStreamerReadError(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	expectedErr := errors.New("some error")
	_, err = NewQueryStreamer(&testReadCloser{
		ReadErr: expectedErr,
	}, "rows", logger)
	require.ErrorIs(t, expectedErr, err)
}

func TestQueryStreamerCloseError(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	expectedError := errors.New("some error")
	streamer, err := NewQueryStreamer(&testReadCloser{
		ReadValue: []byte(`{"rows":[{"test":"value"},{"test2":"value2"}],"meta":{"testkey":"testvalue"}}`),
		CloseErr:  expectedError,
	}, "rows", logger)
	require.NoError(t, err)

	err = streamer.Close()
	require.ErrorIs(t, err, expectedError)
}
