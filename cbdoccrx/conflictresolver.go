package cbdoccrx

type ConflictResolver struct {
	Mode ConflictResolutionMode
}

type Document struct {
	Cas       uint64
	RevNo     uint64
	Expiry    uint32
	Flags     uint32
	IsDeleted bool
	HasXattrs bool
}

func (r ConflictResolver) resolveGeneric(a, b *Document) (ResolveResult, error) {
	if !a.IsDeleted && b.IsDeleted {
		return ResolveResultKeepA, nil
	} else if a.IsDeleted && !b.IsDeleted {
		return ResolveResultKeepB, nil
	}

	if b.Expiry > a.Expiry {
		return ResolveResultKeepB, nil
	} else if a.Expiry > b.Expiry {
		return ResolveResultKeepA, nil
	}

	if b.Flags > a.Flags {
		return ResolveResultKeepB, nil
	} else if a.Flags > b.Flags {
		return ResolveResultKeepA, nil
	}

	if b.HasXattrs && !a.HasXattrs {
		return ResolveResultKeepB, nil
	} else if a.HasXattrs && !b.HasXattrs {
		return ResolveResultKeepA, nil
	}

	return ResolveResultEqual, nil
}

func (r ConflictResolver) resolveSeqNo(a, b *Document) (ResolveResult, error) {
	if b.RevNo > a.RevNo {
		return ResolveResultKeepB, nil
	} else if a.RevNo > b.RevNo {
		return ResolveResultKeepA, nil
	}

	if b.Cas > a.Cas {
		return ResolveResultKeepB, nil
	} else if a.Cas > b.Cas {
		return ResolveResultKeepA, nil
	}

	return r.resolveGeneric(a, b)
}

func (r ConflictResolver) resolveLww(a, b *Document) (ResolveResult, error) {
	if b.Cas > a.Cas {
		return ResolveResultKeepB, nil
	} else if a.Cas > b.Cas {
		return ResolveResultKeepA, nil
	}

	if b.RevNo > a.RevNo {
		return ResolveResultKeepB, nil
	} else if a.RevNo > b.RevNo {
		return ResolveResultKeepA, nil
	}

	return r.resolveGeneric(a, b)
}

func (r ConflictResolver) Resolve(a, b *Document) (ResolveResult, error) {
	if a == nil && b == nil {
		return ResolveResultEqual, nil
	} else if a == nil {
		return ResolveResultKeepB, nil
	} else if b == nil {
		return ResolveResultKeepA, nil
	}

	switch r.Mode {
	case ConflictResolutionModeSeqNo:
		return r.resolveSeqNo(a, b)
	case ConflictResolutionModeLww:
		return r.resolveLww(a, b)
	}

	return ResolveResultUnknown, ErrUnsupportedConflictResolutionMode
}
