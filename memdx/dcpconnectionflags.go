package memdx

type DcpConnectionFlags uint32

const (
	DcpConnectionFlagsProducer           = 1 << 0
	DcpConnectionFlagsIncludeXattrs      = 1 << 2
	DcpConnectionFlagsNoValue            = 1 << 3
	DcpConnectionFlagsIncludeDeleteTime  = 1 << 5
	DcpConnectionFlagsUnderlyingDatatype = 1 << 6
	DcpConnectionFlagsDeleteUserXattrs   = 1 << 8
)
