package memdx

import (
	"bytes"
	"encoding/binary"
)

func SplitXattrBlob(value []byte) ([]byte, []byte, error) {
	if len(value) < 4 {
		return nil, nil, protocolError{"xattr blob too small"}
	}

	// The first 4 bytes are the length of the xattr blob
	xattrLen := binary.BigEndian.Uint32(value[:4])
	return value[4 : 4+xattrLen], value[4+xattrLen:], nil
}

func DecodeXattrBlobEntry(buf []byte) ([]byte, []byte, int, error) {
	if len(buf) < 4 {
		return nil, nil, 0, protocolError{"xattr entry too small"}
	}

	// The first 4 bytes are the length of the xattr blob
	xattrLen := binary.BigEndian.Uint32(buf[:4])
	if len(buf) < int(xattrLen)+4 {
		return nil, nil, 0, protocolError{"xattr blob too small"}
	}

	entryBytes := buf[4 : 4+xattrLen]

	nullDelim1Idx := bytes.IndexByte(entryBytes, 0)
	if nullDelim1Idx == -1 {
		return nil, nil, 0, protocolError{"xattr entry missing first null delimiter"}
	}

	entryName := entryBytes[:nullDelim1Idx]
	entryBytes = entryBytes[nullDelim1Idx+1:]

	nullDelim2Idx := bytes.IndexByte(entryBytes, 0)
	if nullDelim2Idx == -1 {
		return nil, nil, 0, protocolError{"xattr entry missing second null delimiter"}
	}

	if nullDelim2Idx != len(entryBytes)-1 {
		return nil, nil, 0, protocolError{"xattr entry has extra data after second null delimiter"}
	}

	entryValue := entryBytes[:nullDelim2Idx]

	return entryName, entryValue, int(4 + xattrLen), nil
}

func IterXattrBlobEntries(buf []byte, cb func([]byte, []byte)) error {
	for len(buf) > 0 {
		name, value, n, err := DecodeXattrBlobEntry(buf)
		if err != nil {
			return err
		}

		cb(name, value)

		buf = buf[n:]
	}

	return nil
}
