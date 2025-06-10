package cbqueryx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"go.uber.org/zap"
)

type Query struct {
	Logger    *zap.Logger
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Auth      cbhttpx.Authenticator
}

func (h Query) NewRequest(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent: h.UserAgent,
		Endpoint:  h.Endpoint,
		Auth:      h.Auth,
	}.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
}

func (h Query) Execute(
	ctx context.Context,
	method, path, contentType string,
	onBehalfOf *cbhttpx.OnBehalfOfInfo,
	body io.Reader,
) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
	if err != nil {
		return nil, err
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

type ResultStream interface {
	EarlyMetaData() *EarlyMetaData
	HasMoreRows() bool
	ReadRow() (json.RawMessage, error)
	MetaData() (*MetaData, error)
}

func (h Query) Query(ctx context.Context, opts *QueryOptions) (ResultStream, error) {
	reqBytes, err := opts.encodeToJson()
	if err != nil {
		return nil, err
	}

	resp, err := h.Execute(ctx, "POST", "/query/service", "application/json", opts.OnBehalfOf, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, err
	}

	return NewQueryRespReader(resp, &QueryRespReaderOptions{
		Logger:          h.Logger,
		Endpoint:        h.Endpoint,
		Statement:       opts.Statement,
		ClientContextId: opts.ClientContextId,
	})
}

type GetAllIndexesOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (h Query) GetAllIndexes(ctx context.Context, opts *GetAllIndexesOptions) ([]Index, error) {
	var where string
	if opts.CollectionName == "" && opts.ScopeName == "" {
		if opts.BucketName != "" {
			encodedBucket, _ := EncodeValue(opts.BucketName)
			where = fmt.Sprintf("(keyspace_id=%s AND bucket_id IS MISSING) OR bucket_id=%s", encodedBucket, encodedBucket)
		} else {
			where = "1=1"
		}
	} else {
		scopeName := normalizeDefaultName(opts.ScopeName)
		collectionName := normalizeDefaultName(opts.CollectionName)

		encodedBucket, _ := EncodeValue(opts.BucketName)
		encodedScope, _ := EncodeValue(scopeName)
		encodedCollection, _ := EncodeValue(collectionName)

		where = fmt.Sprintf("bucket_id=%s AND scope_id=%s AND keyspace_id=%s",
			encodedBucket, encodedScope, encodedCollection)

		if scopeName == "_default" && collectionName == "_default" {
			// When the user is querying for the default collection, we need to capture the index
			// case where there is only a keyspace_id, which implies the index is on the buckets default
			where = fmt.Sprintf("(%s) OR (keyspace_id=%s AND bucket_id IS MISSING)", where, encodedBucket)
		}
	}

	where = fmt.Sprintf("(%s) AND `using`=\"gsi\"", where)
	qs := fmt.Sprintf("SELECT `idx`.* FROM system:indexes AS idx WHERE %s ORDER BY is_primary DESC, name ASC",
		where)

	var queryOpts QueryOptions
	queryOpts.OnBehalfOf = opts.OnBehalfOf
	queryOpts.Statement = qs

	rows, err := h.Query(ctx, &queryOpts)
	if err != nil {
		return nil, err
	}

	var indexes []Index

	for rows.HasMoreRows() {
		rowBytes, err := rows.ReadRow()
		if err != nil {
			return nil, err
		}

		var row queryIndexRowJson
		if err := json.Unmarshal(rowBytes, &row); err != nil {
			return nil, err
		}

		index := Index{
			Name:        row.Name,
			IsPrimary:   row.IsPrimary,
			Using:       row.Using,
			State:       IndexState(row.State),
			KeyspaceId:  row.KeyspaceId,
			NamespaceId: row.NamespaceId,
			IndexKey:    row.IndexKey,
			Condition:   row.Condition,
			Partition:   row.Partition,
			ScopeId:     row.ScopeId,
			BucketId:    row.BucketId,
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
}

type CreatePrimaryIndexOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	NumReplicas    *int32
	Deferred       *bool
	IgnoreIfExists bool
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (h Query) CreatePrimaryIndex(ctx context.Context, opts *CreatePrimaryIndexOptions) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when creating an index")
	}

	qs := "CREATE PRIMARY INDEX"

	if opts.IndexName != "" {
		qs += " " + EncodeIdentifier(opts.IndexName)
	}

	qs += " ON " + buildKeyspace(opts.BucketName, opts.ScopeName, opts.CollectionName)

	with := make(map[string]interface{})
	if opts.Deferred != nil {
		with["defer_build"] = *opts.Deferred
	}

	if opts.NumReplicas != nil {
		with["num_replica"] = *opts.NumReplicas
	}

	if len(with) > 0 {
		withBytes, err := json.Marshal(with)
		if err != nil {
			return err
		}

		qs += " WITH " + string(withBytes)
	}

	var queryOpts QueryOptions
	queryOpts.OnBehalfOf = opts.OnBehalfOf
	queryOpts.Statement = qs

	rows, err := h.Query(ctx, &queryOpts)
	if errors.Is(err, ErrBuildAlreadyInProgress) {
		// this is considered a success
		return nil
	} else if err != nil {
		if opts.IgnoreIfExists && errors.Is(err, ErrIndexExists) {
			return nil
		}

		return err
	}

	for rows.HasMoreRows() {
		_, _ = rows.ReadRow()
	}

	return nil
}

type CreateIndexOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
	NumReplicas    *int32
	Fields         []string
	Deferred       *bool
	IgnoreIfExists bool
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

func (h Query) CreateIndex(ctx context.Context, opts *CreateIndexOptions) error {
	if opts.IndexName == "" {
		return errors.New("must specify index name when creating an index")
	}
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when creating an index")
	}
	if len(opts.Fields) == 0 {
		return errors.New("must specify fields when creating an index")
	}

	qs := "CREATE INDEX"
	qs += " " + EncodeIdentifier(opts.IndexName)

	qs += " ON " + buildKeyspace(opts.BucketName, opts.ScopeName, opts.CollectionName)

	encodedFields := make([]string, len(opts.Fields))
	for fieldIdx, field := range opts.Fields {
		encodedFields[fieldIdx] = EncodeIdentifier(field)
	}
	qs += " (" + strings.Join(encodedFields, ",") + ")"

	with := make(map[string]interface{})
	if opts.Deferred != nil {
		with["defer_build"] = *opts.Deferred
	}

	if opts.NumReplicas != nil {
		with["num_replica"] = *opts.NumReplicas
	}

	if len(with) > 0 {
		withBytes, err := json.Marshal(with)
		if err != nil {
			return err
		}

		qs += " WITH " + string(withBytes)
	}

	var queryOpts QueryOptions
	queryOpts.OnBehalfOf = opts.OnBehalfOf
	queryOpts.Statement = qs

	rows, err := h.Query(ctx, &queryOpts)
	if errors.Is(err, ErrBuildAlreadyInProgress) {
		// this is considered a success
		return nil
	} else if err != nil {
		if opts.IgnoreIfExists && errors.Is(err, ErrIndexExists) {
			return nil
		}

		return err
	}

	for rows.HasMoreRows() {
		_, _ = rows.ReadRow()
	}

	return nil
}

type DropPrimaryIndexOptions struct {
	BucketName        string
	ScopeName         string
	CollectionName    string
	IndexName         string
	IgnoreIfNotExists bool
	OnBehalfOf        *cbhttpx.OnBehalfOfInfo
}

func (h Query) DropPrimaryIndex(ctx context.Context, opts *DropPrimaryIndexOptions) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when dropping an index")
	}

	keyspace := buildKeyspace(opts.BucketName, opts.ScopeName, opts.CollectionName)

	var qs string
	if opts.IndexName == "" {
		qs += "DROP PRIMARY INDEX"
		if opts.IgnoreIfNotExists {
			qs += " IF EXISTS "
		}
		qs += fmt.Sprintf(" ON %s", keyspace)
	} else {
		encodedName := EncodeIdentifier(opts.IndexName)

		if opts.ScopeName != "" || opts.CollectionName != "" {
			qs += fmt.Sprintf("DROP INDEX %s", encodedName)
			qs += fmt.Sprintf(" ON %s", keyspace)
		} else {
			qs += fmt.Sprintf("DROP INDEX %s.%s", keyspace, encodedName)
		}
	}

	var queryOpts QueryOptions
	queryOpts.OnBehalfOf = opts.OnBehalfOf
	queryOpts.Statement = qs

	rows, err := h.Query(ctx, &queryOpts)
	if err != nil {
		if opts.IgnoreIfNotExists && errors.Is(err, ErrIndexNotFound) {
			return nil
		}

		return err
	}

	for rows.HasMoreRows() {
		_, _ = rows.ReadRow()
	}

	return nil
}

type DropIndexOptions struct {
	BucketName        string
	ScopeName         string
	CollectionName    string
	IndexName         string
	IgnoreIfNotExists bool
	OnBehalfOf        *cbhttpx.OnBehalfOfInfo
}

func (h Query) DropIndex(ctx context.Context, opts *DropIndexOptions) error {
	if opts.IndexName == "" {
		return errors.New("must specify index name when dropping an index")
	}
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when dropping an index")
	}

	encodedName := EncodeIdentifier(opts.IndexName)
	keyspace := buildKeyspace(opts.BucketName, opts.ScopeName, opts.CollectionName)

	var qs string
	if opts.ScopeName != "" || opts.CollectionName != "" {
		qs += fmt.Sprintf("DROP INDEX %s", encodedName)
		qs += fmt.Sprintf(" ON %s", keyspace)
	} else {
		qs += fmt.Sprintf("DROP INDEX %s.%s", keyspace, encodedName)
	}

	var queryOpts QueryOptions
	queryOpts.OnBehalfOf = opts.OnBehalfOf
	queryOpts.Statement = qs

	rows, err := h.Query(ctx, &queryOpts)
	if err != nil {
		if opts.IgnoreIfNotExists && errors.Is(err, ErrIndexNotFound) {
			return nil
		}

		return err
	}

	for rows.HasMoreRows() {
		_, _ = rows.ReadRow()
	}

	return nil
}

type BuildDeferredIndexesOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

type DeferredIndexName struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	IndexName      string
}

func (h Query) BuildDeferredIndexes(ctx context.Context, opts *BuildDeferredIndexesOptions) ([]DeferredIndexName, error) {
	getIndexesResp, err := h.GetAllIndexes(ctx, &GetAllIndexesOptions{
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		OnBehalfOf:     opts.OnBehalfOf,
	})
	if err != nil {
		return nil, err
	}

	deferredIndexes := make(map[string][]DeferredIndexName)
	for _, index := range getIndexesResp {
		if index.State == IndexStateDeferred {
			bucket, scope, collection := indexToNamespaceParts(index)
			deferredIndex := DeferredIndexName{
				BucketName:     bucket,
				ScopeName:      scope,
				CollectionName: collection,
				IndexName:      index.Name,
			}

			keyspace := buildKeyspace(deferredIndex.BucketName, deferredIndex.ScopeName, deferredIndex.CollectionName)
			if _, ok := deferredIndexes[keyspace]; !ok {
				deferredIndexes[keyspace] = []DeferredIndexName{}
			}

			deferredIndexes[keyspace] = append(deferredIndexes[keyspace], deferredIndex)
		}
	}

	if len(deferredIndexes) == 0 {
		// If there are no indexes left to build, we can just return success
		return []DeferredIndexName{}, nil
	}

	for keyspace, indexes := range deferredIndexes {
		escapedIndexNames := make([]string, len(indexes))
		for indexIdx, indexName := range indexes {
			escapedIndexNames[indexIdx] = EncodeIdentifier(indexName.IndexName)
		}

		var qs string
		qs += fmt.Sprintf("BUILD INDEX ON %s(%s)",
			keyspace, strings.Join(escapedIndexNames, ","))

		var queryOpts QueryOptions
		queryOpts.OnBehalfOf = opts.OnBehalfOf
		queryOpts.Statement = qs

		rows, err := h.Query(ctx, &queryOpts)
		if errors.Is(err, ErrBuildAlreadyInProgress) || errors.Is(err, ErrBuildFails) {
			// this is considered a success
			break
		} else if err != nil {
			return nil, err
		}

		for rows.HasMoreRows() {
			_, _ = rows.ReadRow()
		}
	}

	for {
		watchIndexesResp, err := h.GetAllIndexes(ctx, &GetAllIndexesOptions{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
		})
		if err != nil {
			return nil, err
		}

		getIndex := func(indexCtx DeferredIndexName) *Index {
			for _, index := range watchIndexesResp {
				_, scope, collection := indexToNamespaceParts(index)
				if index.Name == indexCtx.IndexName &&
					scope == indexCtx.ScopeName &&
					collection == indexCtx.CollectionName {
					return &index
				}
			}
			return nil
		}

		allIndexesBuilding := true
		for _, indexes := range deferredIndexes {
			for _, index := range indexes {
				foundIndex := getIndex(index)
				if foundIndex == nil {
					// if the index is not found at all, just consider it building
					h.Logger.Warn("an index that was scheduled for building is no longer found",
						zap.String("indexName", index.IndexName))
					continue
				}

				if foundIndex.State == IndexStateDeferred {
					allIndexesBuilding = false
					break
				}
			}
		}

		// if some of the indexes still haven't transitioned out of the deferred state,
		// we wait 100ms and then scan to see if the index has transitioned.
		if !allIndexesBuilding {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			continue
		}

		break
	}

	var indexContexts []DeferredIndexName
	for _, indexes := range deferredIndexes {
		indexContexts = append(indexContexts, indexes...)
	}

	return indexContexts, nil
}

type queryIndexRowJson struct {
	Name        string   `json:"name"`
	IsPrimary   bool     `json:"is_primary"`
	Using       string   `json:"using"`
	State       string   `json:"state"`
	KeyspaceId  string   `json:"keyspace_id"`
	NamespaceId string   `json:"namespace_id"`
	IndexKey    []string `json:"index_key"`
	Condition   string   `json:"condition"`
	Partition   string   `json:"partition"`
	ScopeId     string   `json:"scope_id"`
	BucketId    string   `json:"bucket_id"`
}

func normalizeDefaultName(name string) string {
	resourceName := "_default"
	if name != "" {
		resourceName = name
	}

	return resourceName
}

func buildKeyspace(
	bucket string,
	scope, collection string,
) string {
	if scope != "" && collection != "" {
		return fmt.Sprintf("%s.%s.%s",
			EncodeIdentifier(bucket),
			EncodeIdentifier(scope),
			EncodeIdentifier(collection))
	} else if collection == "" && scope != "" {
		return fmt.Sprintf("%s.%s.%s",
			EncodeIdentifier(bucket),
			EncodeIdentifier(scope),
			EncodeIdentifier("_default"))
	} else if collection != "" && scope == "" {
		return fmt.Sprintf("%s.%s.%s",
			EncodeIdentifier(bucket),
			EncodeIdentifier("_default"),
			EncodeIdentifier(collection))
	}

	return EncodeIdentifier(bucket)
}

func indexToNamespaceParts(index Index) (string, string, string) {
	if index.BucketId == "" {
		defaultScopeColl := "_default"

		return index.KeyspaceId, defaultScopeColl, defaultScopeColl
	}

	return index.BucketId, index.ScopeId, index.KeyspaceId
}
