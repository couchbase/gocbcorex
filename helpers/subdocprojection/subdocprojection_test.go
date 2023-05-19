package subdocprojection

import (
	"testing"

	"github.com/couchbase/gocbcorex/helpers/subdocpath"
	"github.com/stretchr/testify/require"
)

func parsePath(t *testing.T, path string) []subdocpath.Element {
	parts, err := subdocpath.Parse(path)
	if err != nil {
		t.Errorf("failed to parse test path: %s", err.Error())
	}

	return parts
}

func TestProjectorReads(t *testing.T) {
	p := Projector{}
	require.Nil(t, p.Init([]byte(`{"a":{"b":{"c":{"d":1},"f":[9,11,{"c":"c"}],"z":14}},"x":{"`+"y.`z"+`":77}}`)))

	checkPath := func(path string, expected string) {
		res, err := p.Get(parsePath(t, path))
		require.NoError(t, err)
		require.JSONEq(t, expected, string(res))
	}

	checkPath("a.b.c", `{"d":1}`)
	checkPath("a.b.f[0]", `9`)
	checkPath("a.b.f[1]", `11`)
	checkPath("a.b.f[2].c", `"c"`)
	checkPath("a.b.z", `14`)
	checkPath("x.`y.``z`", `77`)
}

func TestProjectorWrites(t *testing.T) {
	p := Projector{}

	checkPath := func(path string, expected string) {
		err := p.Set(parsePath(t, path), []byte(expected))
		require.NoError(t, err)
	}

	checkPath("a.b.c", `{"d":1}`)
	checkPath("a.b.f[0]", `9`)
	checkPath("a.b.z", `14`)
	checkPath("a.c[1].c", `"c"`)
	checkPath("a.c[0].a", `"a"`)
	checkPath("a.c[0].b", `"b"`)
	checkPath("x.`y.``z`", `77`)

	bytes, err := p.Build()
	require.NoError(t, err)
	require.JSONEq(t, `{"a":{"b":{"c":{"d":1},"f":[9],"z":14},"c":[{"a":"a","b":"b"},{"c":"c"}]},"x":{"`+"y.`z"+`":77}}`, string(bytes))
}
