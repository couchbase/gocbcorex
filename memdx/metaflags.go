package memdx

type MetaOpFlag uint32

const (
	MetaOpFlagForceWithMetaOp        = MetaOpFlag(0x01)
	MetaOpFlagForceAcceptWithMetaOps = MetaOpFlag(0x02)
	MetaOpFlagRegenerateCas          = MetaOpFlag(0x04)
	MetaOpFlagSkipConflictResolution = MetaOpFlag(0x08)
	MetaOpFlagIsExpiration           = MetaOpFlag(0x10)
)
