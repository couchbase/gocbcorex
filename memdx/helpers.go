package memdx

import (
	"math"
)

func AppendCollectionID(buf []byte, collectionID uint32) ([]byte, error) {
	return AppendULEB128_32(buf, collectionID), nil
}

func DecodeCollectionID(buf []byte) (uint32, int, error) {
	return DecodeULEB128_32(buf)
}

func EncodeVarDuration(val uint64) (uint16, error) {
	v := (int64)(math.Pow(float64(val)*2, 1/1.74))
	if v >= 0 && v <= 65535 {
		return uint16(v), nil
	}
	return 0, protocolError{"bad variable duration range"}
}

func DecodeVarDuration(val uint16) uint64 {
	return (uint64)(math.Pow(float64(val), 1.74) / 2)
}

func AppendCollectionIDAndKey(collectionID uint32, key []byte, buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, 0, 5+len(key))
	}

	buf, err := AppendCollectionID(buf, collectionID)
	if err != nil {
		return nil, err
	}

	buf = append(buf, key...)
	return buf, nil
}

func DecodeCollectionIDAndKey(buf []byte) (uint32, []byte, error) {
	collectionID, n, err := DecodeCollectionID(buf)
	if err != nil {
		return 0, nil, err
	}

	return collectionID, buf[n:], nil
}
