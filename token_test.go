package gocbcorex

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMutationState_Add(t *testing.T) {
	bucketName := "frank"

	fakeToken1 := MutationToken{
		VbID:   1,
		VbUuid: 9,
		SeqNo:  12,
	}
	fakeToken2 := MutationToken{
		VbID:   2,
		VbUuid: 1,
		SeqNo:  22,
	}
	fakeToken3 := MutationToken{
		VbID:   2,
		VbUuid: 4,
		SeqNo:  99,
	}

	state := NewMutationState(bucketName, fakeToken1, fakeToken2)
	state.Add(bucketName, fakeToken3)

	bytes, err := json.Marshal(&state)
	require.NoError(t, err)

	require.Equal(t, 0, strings.Compare(string(bytes), "{\"frank\":{\"1\":[12,\"9\"],\"2\":[99,\"4\"]}}"),
		"marshalled value was not as expected")

	// So as to avoid testing on private properties we'll check if unmarshal works by marshaling the result.
	var afterState MutationState
	err = json.Unmarshal(bytes, &afterState)
	require.NoError(t, err)

	bytes, err = json.Marshal(&state)
	require.NoError(t, err)

	require.Equal(t, 0, strings.Compare(string(bytes), "{\"frank\":{\"1\":[12,\"9\"],\"2\":[99,\"4\"]}}"),
		"unmarshalled value was not as expected")
}

func TestMutationState_toSearchMutationState(t *testing.T) {
	bucketName := "frank"

	fakeToken1 := MutationToken{
		VbID:   1,
		VbUuid: 9,
		SeqNo:  12,
	}
	fakeToken2 := MutationToken{
		VbID:   2,
		VbUuid: 1,
		SeqNo:  22,
	}

	state := NewMutationState(bucketName, fakeToken1, fakeToken2)

	searchToken := state.toSearchMutationState("frankindex")

	// What we actually care about is the format once marshaled.
	bytes, err := json.Marshal(&searchToken)
	require.NoError(t, err)

	require.Equal(t, 0, strings.Compare(string(bytes), "{\"frankindex\":{\"1/9\":12,\"2/1\":22}}"),
		"marshalled value was not as expected")
}
