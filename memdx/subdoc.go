package memdx

type SubdocOp interface {
	IsXattrOp() bool
}

// ReorderSubdocOps can be used to reorder a list of SubdocOp so that xattrs
// are at the start of the list - as required by the server.
func ReorderSubdocOps[T SubdocOp](ops []T) (reordered []T, indexes []int) {
	var xAttrOps []T
	var xAttrIndexes []int
	var sops []T
	var opIndexes []int
	for i, op := range ops {
		if op.IsXattrOp() {
			xAttrOps = append(xAttrOps, op)
			xAttrIndexes = append(xAttrIndexes, i)
		} else {
			sops = append(sops, op)
			opIndexes = append(opIndexes, i)
		}
	}

	return sops, opIndexes
}

// SubDocResult encapsulates the results from a single sub-document operation.
type SubDocResult struct {
	Err   error
	Value []byte
}

// LookupInOp defines a per-operation structure to be passed to LookupIn
// for performing many sub-document operations.
type LookupInOp struct {
	Op    LookupInOpType
	Flags SubdocOpFlag
	Path  []byte
}

func (op LookupInOp) IsXattrOp() bool {
	return op.Flags&SubdocOpFlagXattrPath != 0
}

// MutateInOp defines a per-operation structure to be passed to MutateIn
// for performing many sub-document operations.
type MutateInOp struct {
	Op    MutateInOpType
	Flags SubdocOpFlag
	Path  []byte
	Value []byte
}

func (op MutateInOp) IsXattrOp() bool {
	return op.Flags&SubdocOpFlagXattrPath != 0
}

// SubdocOpFlag specifies flags for a sub-document operation.
type SubdocOpFlag uint8

const (
	// SubdocOpFlagNone indicates no special treatment for this operation.
	SubdocOpFlagNone = SubdocOpFlag(0x00)

	// SubdocOpFlagMkDirP indicates that the path should be created if it does not already exist.
	SubdocOpFlagMkDirP = SubdocOpFlag(0x01)

	// 0x02 is unused, formally SubdocFlagMkDoc

	// SubdocOpFlagXattrPath indicates that the path refers to an Xattr rather than the document body.
	SubdocOpFlagXattrPath = SubdocOpFlag(0x04)

	// 0x08 is unused, formally SubdocFlagAccessDeleted

	// SubdocOpFlagExpandMacros indicates that the value portion of any sub-document mutations
	// should be expanded if they contain macros such as ${Mutation.CAS}.
	SubdocOpFlagExpandMacros = SubdocOpFlag(0x10)
)

// LookupInOpType specifies the type of lookup in operation.
type LookupInOpType uint8

const (
	// LookupInOpTypeGet indicates the operation is a sub-document `Get` operation.
	LookupInOpTypeGet = LookupInOpType(OpCodeSubDocGet)

	// LookupInOpTypeExists indicates the operation is a sub-document `Exists` operation.
	LookupInOpTypeExists = LookupInOpType(OpCodeSubDocExists)

	// LookupInOpTypeGetCount indicates the operation is a sub-document `GetCount` operation.
	LookupInOpTypeGetCount = LookupInOpType(OpCodeSubDocGetCount)

	// LookupInOpTypeGetDoc represents a full document retrieval, for use with extended attribute ops.
	LookupInOpTypeGetDoc = LookupInOpType(OpCodeGet)
)

// MutateInOpType specifies the type of mutate in operation.
type MutateInOpType uint8

const (
	// MutateInOpTypeDictAdd indicates the operation is a sub-document `Add` operation.
	MutateInOpTypeDictAdd = MutateInOpType(OpCodeSubDocDictAdd)

	// MutateInOpTypeDictSet indicates the operation is a sub-document `Set` operation.
	MutateInOpTypeDictSet = MutateInOpType(OpCodeSubDocDictSet)

	// MutateInOpTypeDelete indicates the operation is a sub-document `Remove` operation.
	MutateInOpTypeDelete = MutateInOpType(OpCodeSubDocDelete)

	// MutateInOpTypeReplace indicates the operation is a sub-document `Replace` operation.
	MutateInOpTypeReplace = MutateInOpType(OpCodeSubDocReplace)

	// MutateInOpTypeArrayPushLast indicates the operation is a sub-document `ArrayPushLast` operation.
	MutateInOpTypeArrayPushLast = MutateInOpType(OpCodeSubDocArrayPushLast)

	// MutateInOpTypeArrayPushFirst indicates the operation is a sub-document `ArrayPushFirst` operation.
	MutateInOpTypeArrayPushFirst = MutateInOpType(OpCodeSubDocArrayPushFirst)

	// MutateInOpTypeArrayInsert indicates the operation is a sub-document `ArrayInsert` operation.
	MutateInOpTypeArrayInsert = MutateInOpType(OpCodeSubDocArrayInsert)

	// MutateInOpTypeArrayAddUnique indicates the operation is a sub-document `ArrayAddUnique` operation.
	MutateInOpTypeArrayAddUnique = MutateInOpType(OpCodeSubDocArrayAddUnique)

	// MutateInOpTypeCounter indicates the operation is a sub-document `Counter` operation.
	MutateInOpTypeCounter = MutateInOpType(OpCodeSubDocCounter)

	// MutateInOpTypeSetDoc represents a full document set, for use with extended attribute ops.
	MutateInOpTypeSetDoc = MutateInOpType(OpCodeSet)

	// MutateInOpTypeAddDoc represents a full document add, for use with extended attribute ops.
	MutateInOpTypeAddDoc = MutateInOpType(OpCodeAdd)

	// MutateInOpTypeDeleteDoc represents a full document delete, for use with extended attribute ops.
	MutateInOpTypeDeleteDoc = MutateInOpType(OpCodeDelete)

	// MutateInOpTypeReplaceBodyWithXattr represents a replace body with xattr op.
	// Uncommitted: This API may change in the future.
	MutateInOpTypeReplaceBodyWithXattr = MutateInOpType(OpCodeSubDocReplaceBodyWithXattr)
)

// SubdocDocFlag specifies document-level flags for a sub-document operation.
type SubdocDocFlag uint8

const (
	// SubdocDocFlagNone indicates no special treatment for this operation.
	SubdocDocFlagNone = SubdocDocFlag(0x00)

	// SubdocDocFlagMkDoc indicates that the document should be created if it does not already exist.
	SubdocDocFlagMkDoc = SubdocDocFlag(0x01)

	// SubdocDocFlagAddDoc indices that this operation should be an add rather than set.
	SubdocDocFlagAddDoc = SubdocDocFlag(0x02)

	// SubdocDocFlagAccessDeleted indicates that you wish to receive soft-deleted documents.
	// Internal: This should never be used and is not supported.
	SubdocDocFlagAccessDeleted = SubdocDocFlag(0x04)

	// SubdocDocFlagCreateAsDeleted indicates that the document should be created as deleted.
	// That is, to create a tombstone only.
	// Internal: This should never be used and is not supported.
	SubdocDocFlagCreateAsDeleted = SubdocDocFlag(0x08)
)
