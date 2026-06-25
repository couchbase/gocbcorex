package cbmgmtx

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseForInvalidArg(t *testing.T) {
	errTextStart := `{"errors":{`
	errTextEnd := `},"summaries":{"ramSummary":{"total":3028287488,"otherBuckets":0,
	"nodesCount":1,"perNodeMegs":100,"thisAlloc":104857600,"thisUsed":0,"free":2923429888},
	"hddSummary":{"total":63089455104,"otherData":5047156408,"otherBuckets":0,"thisUsed":0,
	"free":58042298696}}}`

	t.Run("single field in chain", func(t *testing.T) {
		errText := errTextStart + `"fieldOne":"reasonOne"` + errTextEnd
		sErr := parseForInvalidArg(errText)
		assert.Equal(t, "fieldOne", sErr.Argument)
		assert.Equal(t, "reasonOne", sErr.Reason)
	})

	t.Run("multiple fields in chain", func(t *testing.T) {
		errText := errTextStart + `"fieldOne":"reasonOne","fieldTwo":"reasonTwo"` + errTextEnd
		sErr := parseForInvalidArg(errText)
		isFirstError := sErr.Argument == "fieldOne"
		if isFirstError {
			assert.Equal(t, "reasonOne", sErr.Reason)
		} else {
			assert.Equal(t, "fieldTwo", sErr.Argument)
			assert.Equal(t, "reasonTwo", sErr.Reason)
		}
	})

	t.Run("single field in chain - commas in reason", func(t *testing.T) {
		errText := errTextStart + `"fieldOne":"reasonOne, something else"` + errTextEnd
		sErr := parseForInvalidArg(errText)
		assert.Equal(t, "fieldOne", sErr.Argument)
		assert.Equal(t, "reasonOne, something else", sErr.Reason)
	})

	t.Run("multiple fields in chain - commas in reasons", func(t *testing.T) {
		errText := errTextStart + `"fieldOne":"reasonOne, something else","fieldTwo":"reason, something"` + errTextEnd
		sErr := parseForInvalidArg(errText)
		isFirstError := sErr.Argument == "fieldOne"
		if isFirstError {
			assert.Equal(t, "reasonOne, something else", sErr.Reason)
		} else {
			assert.Equal(t, "fieldTwo", sErr.Argument)
			assert.Equal(t, "reason, something", sErr.Reason)
		}
	})
}

func TestDecodeCommonError(t *testing.T) {
	mgmt := Management{}

	t.Run("group not found", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: 404,
			Body:       io.NopCloser(strings.NewReader("Group not found")),
		}
		err := mgmt.DecodeCommonError(resp)
		assert.ErrorIs(t, err, ErrGroupNotFound)
	})

	t.Run("unknown group", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: 404,
			Body:       io.NopCloser(strings.NewReader("Unknown group.")),
		}
		err := mgmt.DecodeCommonError(resp)
		assert.ErrorIs(t, err, ErrGroupNotFound)
	})

	t.Run("groups do not exist", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: 400,
			Body:       io.NopCloser(strings.NewReader(`{"errors":{"groups":"Groups do not exist: non-existent-group-name"}}`)),
		}
		err := mgmt.DecodeCommonError(resp)
		assert.ErrorIs(t, err, ErrGroupNotFound)
	})
}
