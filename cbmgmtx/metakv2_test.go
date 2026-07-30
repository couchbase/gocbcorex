package cbmgmtx

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeMetaKv2Path(t *testing.T) {
	assert.Equal(t, "/foo/bar", normalizeMetaKv2Path("foo/bar"))
	assert.Equal(t, "/foo/bar/", normalizeMetaKv2Path("/foo/bar/"))
	assert.Equal(t, "/", normalizeMetaKv2Path("/"))
}

func TestParseMetaKv2Directory(t *testing.T) {
	jsonPayload := `{
		"/root/subdir/": {
			"revision": "dir-rev-1",
			"value": {
				"/root/subdir/key1": {
					"revision": "rev-key-1",
					"value": "val1"
				},
				"/root/subdir/nested/": {
					"revision": "dir-rev-2",
					"value": {
						"/root/subdir/nested/key2": {
							"revision": "rev-key-2",
							"value": "val2"
						}
					}
				}
			}
		}
	}`

	entries := make(map[string]MetaKv2Entry)
	err := parseMetaKv2Directory(json.RawMessage(jsonPayload), entries)
	require.NoError(t, err)

	assert.Len(t, entries, 4)

	assert.True(t, entries["/root/subdir/"].IsDir)
	assert.Equal(t, "dir-rev-1", entries["/root/subdir/"].Revision)

	assert.False(t, entries["/root/subdir/key1"].IsDir)
	assert.Equal(t, "rev-key-1", entries["/root/subdir/key1"].Revision)
	assert.Equal(t, []byte("val1"), entries["/root/subdir/key1"].Value)

	assert.True(t, entries["/root/subdir/nested/"].IsDir)
	assert.Equal(t, "dir-rev-2", entries["/root/subdir/nested/"].Revision)

	assert.False(t, entries["/root/subdir/nested/key2"].IsDir)
	assert.Equal(t, "rev-key-2", entries["/root/subdir/nested/key2"].Revision)
	assert.Equal(t, []byte("val2"), entries["/root/subdir/nested/key2"].Value)
}
