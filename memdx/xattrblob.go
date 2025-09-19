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

func JoinXattrBlob(xattrEntries, docValue []byte) []byte {
	totalLen := 4 + len(xattrEntries) + len(docValue)
	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(xattrEntries)))
	copy(buf[4:], xattrEntries)
	copy(buf[4+len(xattrEntries):], docValue)
	return buf
}

func DecodeXattrBlobEntry(buf []byte) (string, string, int, error) {
	if len(buf) < 4 {
		return "", "", 0, protocolError{"xattr entry too small"}
	}

	// The first 4 bytes are the length of the xattr blob
	xattrLen := binary.BigEndian.Uint32(buf[:4])
	if len(buf) < int(xattrLen)+4 {
		return "", "", 0, protocolError{"xattr blob too small"}
	}

	entryBytes := buf[4 : 4+xattrLen]

	nullDelim1Idx := bytes.IndexByte(entryBytes, 0)
	if nullDelim1Idx == -1 {
		return "", "", 0, protocolError{"xattr entry missing first null delimiter"}
	}

	entryName := string(entryBytes[:nullDelim1Idx])
	entryBytes = entryBytes[nullDelim1Idx+1:]

	nullDelim2Idx := bytes.IndexByte(entryBytes, 0)
	if nullDelim2Idx == -1 {
		return "", "", 0, protocolError{"xattr entry missing second null delimiter"}
	}

	if nullDelim2Idx != len(entryBytes)-1 {
		return "", "", 0, protocolError{"xattr entry has extra data after second null delimiter"}
	}

	entryValue := string(entryBytes[:nullDelim2Idx])

	return entryName, entryValue, int(4 + xattrLen), nil
}

func AppendXattrBlobEntry(buf []byte, name, value string) []byte {
	nameBytes := []byte(name)
	valueBytes := []byte(value)

	entryLen := len(nameBytes) + 1 + len(valueBytes) + 1

	entryLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(entryLenBytes, uint32(entryLen))

	buf = append(buf, entryLenBytes...)
	buf = append(buf, nameBytes...)
	buf = append(buf, 0)
	buf = append(buf, valueBytes...)
	buf = append(buf, 0)

	return buf
}

func IterXattrBlobEntries(buf []byte, cb func(string, string)) error {
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
