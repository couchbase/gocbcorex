package core

import "github.com/couchbase/gocbcorex/memdx"

type CompressionManager interface {
	Compress(bool, memdx.DatatypeFlag, []byte) ([]byte, memdx.DatatypeFlag, error)
	Decompress(memdx.DatatypeFlag, []byte) ([]byte, memdx.DatatypeFlag, error)
}
