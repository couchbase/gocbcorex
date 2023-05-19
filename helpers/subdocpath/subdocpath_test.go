package subdocpath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	testOne := func(path string, output []Element) {
		parts, err := Parse(path)
		if output == nil {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
			if len(output) == 0 {
				require.Len(t, output, 0)
			} else {
				require.Equal(t, output, parts)
			}
		}
	}

	testOne("x.y.z", []Element{
		{Key: "x"},
		{Key: "y"},
		{Key: "z"},
	})
	testOne("[1]", []Element{
		{Index: 1},
	})
	testOne("x[1]", []Element{
		{Key: "x"},
		{Index: 1},
	})
	testOne("a.x[1]", []Element{
		{Key: "a"},
		{Key: "x"},
		{Index: 1},
	})
	testOne("a.x[1].z", []Element{
		{Key: "a"},
		{Key: "x"},
		{Index: 1},
		{Key: "z"},
	})
	testOne("x[-1]", []Element{
		{Key: "x"},
		{Index: -1},
	})
	testOne("x.`y.``z`", []Element{
		{Key: "x"},
		{Key: "y.`z"},
	})
	testOne("", []Element{})
	testOne("x..z", nil)
	testOne("x[1]]", nil)
	testOne("x]", nil)
	testOne("x.", nil)
	testOne(".", nil)
}
