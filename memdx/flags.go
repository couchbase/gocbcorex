package memdx

// DatatypeFlag specifies data flags for the value of a document.
type DatatypeFlag uint8

const (
	// DatatypeFlagJSON indicates the server believes the value payload to be JSON.
	DatatypeFlagJSON = DatatypeFlag(0x01)

	// DatatypeFlagCompressed indicates the value payload is compressed.
	DatatypeFlagCompressed = DatatypeFlag(0x02)

	// DatatypeFlagXattrs indicates the inclusion of xattr data in the value payload.
	DatatypeFlagXattrs = DatatypeFlag(0x04)
)
