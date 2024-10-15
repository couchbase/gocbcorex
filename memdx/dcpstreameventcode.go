package memdx

type DcpStreamEventCode uint32

const (
	DcpStreamEventCodeCollectionCreate  DcpStreamEventCode = 0x00
	DcpStreamEventCodeCollectionDelete  DcpStreamEventCode = 0x01
	DcpStreamEventCodeCollectionFlush   DcpStreamEventCode = 0x02
	DcpStreamEventCodeScopeCreate       DcpStreamEventCode = 0x03
	DcpStreamEventCodeScopeDelete       DcpStreamEventCode = 0x04
	DcpStreamEventCodeCollectionChanged DcpStreamEventCode = 0x05
)
