package core

import (
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/golang/snappy"
)

type CompressionManagerDefault struct {
	compressionMinSize  int
	compressionMinRatio float64

	// Some users require the ability to disable decompressing values. e.g. if they read docs from
	// the server and then want to store them compressed as a backup.
	disableDecompression bool
}

func (cmd *CompressionManagerDefault) Compress(supportsSnappy bool, datatype memdx.DatatypeFlag, value []byte) ([]byte, memdx.DatatypeFlag, error) {
	if !supportsSnappy {
		return value, datatype, nil
	}

	// If the packet is already compressed then we don't want to compress it again.
	if (datatype & memdx.DatatypeFlagCompressed) != 0 {
		return value, datatype, nil
	}

	packetSize := len(value)
	// Only compress values that are large enough to worthwhile.
	if packetSize <= cmd.compressionMinSize {
		return value, datatype, nil
	}

	compressedValue := snappy.Encode(nil, value)
	// Only return the compressed value if the ratio of compressed:original is small enough.
	if float64(len(compressedValue))/float64(packetSize) > cmd.compressionMinRatio {
		return value, datatype, nil
	}

	return compressedValue, datatype | memdx.DatatypeFlagCompressed, nil
}

func (cmd *CompressionManagerDefault) Decompress(datatype memdx.DatatypeFlag, value []byte) ([]byte, memdx.DatatypeFlag, error) {
	if cmd.disableDecompression {
		return value, datatype, nil
	}

	if (datatype & memdx.DatatypeFlagCompressed) == 0 {
		return value, datatype, nil
	}

	newValue, err := snappy.Decode(nil, value)
	if err != nil {
		return nil, 0, err
	}

	return newValue, datatype & ^memdx.DatatypeFlagCompressed, nil
}
