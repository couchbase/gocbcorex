package cbdoccrx

type ResolveResult int

const (
	ResolveResultUnknown ResolveResult = iota
	ResolveResultKeepA
	ResolveResultKeepB
	ResolveResultEqual
)
