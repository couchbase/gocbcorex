package commonflags

const (
	// Legacy flag format for JSON data.
	LegacyJSON = 0

	// Common flags mask
	Mask = 0xFF000000
	// Common flags mask for data format
	FmtMask = 0x0F000000
	// Common flags mask for compression mode.
	CmprMask = 0xE0000000

	// Common flag format for sdk-private data.
	FmtPrivate = 1 << 24 // nolint: deadcode,varcheck,unused
	// Common flag format for JSON data.
	FmtJSON = 2 << 24
	// Common flag format for binary data.
	FmtBinary = 3 << 24
	// Common flag format for string data.
	FmtString = 4 << 24

	// Common flags compression for disabled compression.
	CmprNone = 0 << 29
)

// DataType represents the type of data for a value
type DataType uint32

// CompressionType indicates the type of compression for a value
type CompressionType uint32

const (
	// UnknownType indicates the values type is unknown.
	UnknownType = DataType(0)

	// JSONType indicates the value is JSON data.
	JSONType = DataType(1)

	// BinaryType indicates the value is binary data.
	BinaryType = DataType(2)

	// StringType indicates the value is string data.
	StringType = DataType(3)
)

const (
	// UnknownCompression indicates that the compression type is unknown.
	UnknownCompression = CompressionType(0)

	// NoCompression indicates that no compression is being used.
	NoCompression = CompressionType(1)
)

// Encode encodes a data type and compression type into a flags
// value using the common flags specification.
func Encode(valueType DataType, compression CompressionType) uint32 {
	var flags uint32

	switch valueType {
	case JSONType:
		flags |= FmtJSON
	case BinaryType:
		flags |= FmtBinary
	case StringType:
		flags |= FmtString
	case UnknownType:
		// flags |= ?
	}

	switch compression {
	case NoCompression:
		// flags |= 0
	case UnknownCompression:
		// flags |= ?
	}

	return flags
}

// Decode decodes a flags value into a data type and compression type
// using the common flags specification.
func Decode(flags uint32) (DataType, CompressionType) {
	// Check for legacy flags
	if flags&Mask == 0 {
		// Legacy Flags
		if flags == LegacyJSON {
			// Legacy JSON
			flags = FmtJSON
		} else {
			return UnknownType, UnknownCompression
		}
	}

	valueType := UnknownType
	compression := UnknownCompression

	switch flags & FmtMask {
	case FmtBinary:
		valueType = BinaryType
	case FmtString:
		valueType = StringType
	case FmtJSON:
		valueType = JSONType
	}

	switch flags & CmprMask {
	case CmprNone:
		compression = NoCompression
	}

	return valueType, compression
}
