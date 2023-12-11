package cbqueryx

type Index struct {
	Name        string
	IsPrimary   bool
	Using       string
	State       IndexState
	KeyspaceId  string
	NamespaceId string
	IndexKey    []string
	Condition   string
	Partition   string
	ScopeId     string
	BucketId    string
}

type IndexState string

const (
	IndexStateDeferred  IndexState = "deferred"
	IndexStateBuilding  IndexState = "building"
	IndexStatePending   IndexState = "pending"
	IndexStateOnline    IndexState = "online"
	IndexStateOffline   IndexState = "offline"
	IndexStateAbridged  IndexState = "abridged"
	IndexStateScheduled IndexState = "scheduled"
)
