package cbmgmtx

import (
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
			assert.Equal(t, sErr.Reason, "reasonOne")
		} else {
			assert.Equal(t, sErr.Argument, "fieldTwo")
			assert.Equal(t, sErr.Reason, "reasonTwo")
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
			assert.Equal(t, sErr.Reason, "reasonOne, something else")
		} else {
			assert.Equal(t, sErr.Argument, "fieldTwo")
			assert.Equal(t, sErr.Reason, "reason, something")
		}
	})
}
