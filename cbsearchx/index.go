package cbsearchx

import "encoding/json"

// Index is used to define a search index.
type Index struct {
	// Name represents the name of this index.
	Name string
	// Params are index properties such as store type and mappings.
	Params map[string]json.RawMessage
	// PlanParams are plan properties such as number of replicas and number of partitions.
	PlanParams map[string]json.RawMessage
	// PrevIndexUUID is intended for clients that want to check that they are not overwriting the index definition updates of concurrent clients.
	PrevIndexUUID string
	// SourceName is the name of the source of the data for the index e.g. bucket name.
	SourceName string
	// SourceParams are extra parameters to be defined. These are usually things like advanced connection and tuning
	// parameters.
	SourceParams map[string]json.RawMessage
	// SourceType is the type of the data source, e.g. couchbase or nil depending on the Type field.
	SourceType string
	// SourceUUID is the UUID of the data source, this can be used to more tightly tie the index to a source.
	SourceUUID string
	// Type is the type of index, e.g. fulltext-index or fulltext-alias.
	Type string
	// UUID is required for updates. It provides a means of ensuring consistency, the UUID must match the UUID value
	// for the index on the server.
	UUID string
}

func NewIndex(name, indexType string) Index {
	return Index{
		Name: name,
		Type: indexType,
	}
}
