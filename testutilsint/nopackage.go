package testutilsint

// we intentionally import a bunch of our sub-packages to avoid accidentally
// using these functions from non-integration files.

import (
	_ "github.com/couchbase/gocbcorex"
	_ "github.com/couchbase/gocbcorex/cbauthx"
	_ "github.com/couchbase/gocbcorex/cbhttpx"
	_ "github.com/couchbase/gocbcorex/cbqueryx"
	_ "github.com/couchbase/gocbcorex/cbsearchx"
	_ "github.com/couchbase/gocbcorex/memdx"
)
