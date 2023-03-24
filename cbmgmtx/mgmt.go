package cbmgmtx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type Management struct {
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Username  string
	Password  string
}

func (h Management) NewRequest(
	ctx context.Context,
	method, path, contentType, onBehalfOf string,
	body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent:     h.UserAgent,
		Endpoint:      h.Endpoint,
		BasicAuthUser: h.Username,
		BasicAuthPass: h.Password,
	}.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
}

func (h Management) Execute(ctx context.Context, method, path, contentType, onBehalfOf string, body io.Reader) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, onBehalfOf, body)
	if err != nil {
		return nil, err
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

func (h Management) DecodeCommonError(resp *http.Response) error {
	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return contextualError{
			Description: "failed to read error body for non-success response",
			Cause:       readErr,
		}
	}

	var err error
	errText := strings.ToLower(string(bodyBytes))

	if strings.Contains(errText, "not found") && strings.Contains(errText, "collection") {
		err = ErrCollectionNotFound
	} else if strings.Contains(errText, "not found") && strings.Contains(errText, "scope") {
		err = ErrScopeNotFound
	} else if strings.Contains(errText, "not found") && strings.Contains(errText, "bucket") {
		err = ErrBucketNotFound
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "collection") {
		err = ErrCollectionExists
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "scope") {
		err = ErrScopeExists
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "bucket") {
		err = ErrBucketExists
	} else if strings.Contains(errText, "requested resource not found") {
		// if we get this specific error, and its none of the above errors, then this indicates
		// that it was the top level resource which could not be found, which is the bucket.
		err = ErrBucketNotFound
	} else if resp.StatusCode == 404 {
		err = ErrUnsupportedFeature
	} else if resp.StatusCode == 401 {
		err = ErrAccessDenied
	}

	if err == nil {
		err = errors.New("unexpected error response")
	}

	return ServerError{
		Cause:      err,
		StatusCode: resp.StatusCode,
		Body:       bodyBytes,
	}
}

type GetClusterConfigOptions struct {
	OnBehalfOf string
}

func (h Management) GetClusterConfig(ctx context.Context, opts *GetClusterConfigOptions) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.FullConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type GetTerseClusterConfigOptions struct {
	OnBehalfOf string
}

func (h Management) GetTerseClusterConfig(ctx context.Context, opts *GetTerseClusterConfigOptions) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default/nodeServices", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.TerseConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type StreamTerseClusterConfigOptions struct {
	OnBehalfOf string
}

type TerseClusterConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseClusterConfig(ctx context.Context, opts *StreamTerseClusterConfigOptions) (TerseClusterConfig_Stream, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default/nodeServicesStreaming", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.TerseConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}, nil
}

type GetBucketConfigOptions struct {
	BucketName string
	OnBehalfOf string
}

func (h Management) GetBucketConfig(ctx context.Context, opts *GetBucketConfigOptions) (*cbconfig.FullConfigJson, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when fetching a bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s", opts.BucketName), "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.FullConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type GetTerseBucketConfigOptions struct {
	BucketName string
	OnBehalfOf string
}

func (h Management) GetTerseBucketConfig(ctx context.Context, opts *GetTerseBucketConfigOptions) (*cbconfig.TerseConfigJson, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when fetching a terse bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/b/%s", opts.BucketName), "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.TerseConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type StreamTerseBucketConfigOptions struct {
	BucketName string
	OnBehalfOf string
}

type TerseBucketConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseBucketConfig(ctx context.Context, opts *StreamTerseBucketConfigOptions) (TerseBucketConfig_Stream, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when streaming a bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/bs/%s", opts.BucketName), "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.TerseConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}, nil
}

type GetCollectionManifestOptions struct {
	BucketName string
	OnBehalfOf string
}

type CollectionManifestCollectionJson struct {
	UID    string `json:"uid"`
	Name   string `json:"name"`
	MaxTTL uint32 `json:"maxTTL,omitempty"`
}

type CollectionManifestScopeJson struct {
	UID         string                             `json:"uid"`
	Name        string                             `json:"name"`
	Collections []CollectionManifestCollectionJson `json:"collections,omitempty"`
}

type CollectionManifestJson struct {
	UID    string                        `json:"uid"`
	Scopes []CollectionManifestScopeJson `json:"scopes,omitempty"`
}

func (h Management) GetCollectionManifest(ctx context.Context, opts *GetCollectionManifestOptions) (*CollectionManifestJson, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when fetching a collection manifest")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", opts.BucketName), "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return cbhttpx.JsonBlockStreamer[CollectionManifestJson]{
		Decoder: json.NewDecoder(resp.Body),
	}.Recv()
}

type CreateScopeOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf string
}

func (h Management) CreateScope(
	ctx context.Context,
	opts *CreateScopeOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when creating a scope")
	}
	if opts.ScopeName == "" {
		return errors.New("must specify scope name when creating a scope")
	}

	posts := url.Values{}
	posts.Add("name", opts.ScopeName)

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", opts.BucketName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type DeleteScopeOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf string
}

func (h Management) DeleteScope(
	ctx context.Context,
	opts *DeleteScopeOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when deleting a scope")
	}
	if opts.ScopeName == "" {
		return errors.New("must specify scope name when deleting a scope")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", opts.BucketName, opts.ScopeName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type CreateCollectionOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	MaxTTL         uint32
	OnBehalfOf     string
}

func (h Management) CreateCollection(
	ctx context.Context,
	opts *CreateCollectionOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when creating a collection")
	}
	if opts.ScopeName == "" {
		return errors.New("must specify scope name when creating a collection")
	}
	if opts.CollectionName == "" {
		return errors.New("must specify collection name when creating a collection")
	}

	posts := url.Values{}
	posts.Add("name", opts.CollectionName)

	if opts.MaxTTL > 0 {
		posts.Add("maxTTL", fmt.Sprintf("%d", int(opts.MaxTTL)))
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", opts.BucketName, opts.ScopeName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type DeleteCollectionOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     string
}

func (h Management) DeleteCollection(
	ctx context.Context,
	opts *DeleteCollectionOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when deleting a collection")
	}
	if opts.ScopeName == "" {
		return errors.New("must specify scope name when deleting a collection")
	}
	if opts.CollectionName == "" {
		return errors.New("must specify collection name when deleting a collection")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", opts.BucketName, opts.ScopeName, opts.CollectionName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type MutableBucketSettings struct {
	FlushEnabled         bool
	ReplicaIndexDisabled bool
	RAMQuotaMB           uint64
	ReplicaNumber        uint32
	BucketType           BucketType
	EvictionPolicy       EvictionPolicyType
	MaxTTL               time.Duration
	CompressionMode      CompressionMode
	DurabilityMinLevel   DurabilityLevel
	StorageBackend       StorageBackend
}

func (h Management) encodeMutableBucketSettings(posts *url.Values, opts *MutableBucketSettings) error {
	if opts.FlushEnabled {
		posts.Add("flushEnabled", "1")
	} else {
		posts.Add("flushEnabled", "0")
	}
	if opts.BucketType != BucketTypeEphemeral {
		if opts.ReplicaIndexDisabled {
			posts.Add("replicaIndex", "0")
		} else {
			posts.Add("replicaIndex", "1")
		}
	} else {
		return errors.New("cannot specify ReplicaIndexDisabled for Ephemeral buckets")
	}
	if opts.RAMQuotaMB > 0 {
		posts.Add("ramQuotaMB", fmt.Sprintf("%d", opts.RAMQuotaMB))
	}
	// we always write the replicaNumber since 0 means "default"
	if true {
		posts.Add("replicaNumber", fmt.Sprintf("%d", opts.ReplicaNumber))
	}
	if opts.BucketType != BucketTypeUnset {
		posts.Add("bucketType", string(opts.BucketType))
	}
	if opts.EvictionPolicy != "" {
		posts.Add("evictionPolicy", string(opts.EvictionPolicy))
	}
	if opts.MaxTTL > 0 {
		posts.Add("maxTTL", fmt.Sprintf("%d", opts.MaxTTL/time.Second))
	}
	if opts.CompressionMode != "" {
		posts.Add("compressionMode", string(opts.CompressionMode))
	}
	if opts.DurabilityMinLevel != DurabilityLevelUnset {
		posts.Add("durabilityMinLevel", string(opts.DurabilityMinLevel))
	}
	if opts.StorageBackend != "" {
		posts.Add("storageBackend", string(opts.StorageBackend))
	}

	return nil
}

func (h Management) decodeMutableBucketSettings(data *bucketSettingsJson) (*MutableBucketSettings, error) {
	settings := MutableBucketSettings{}

	settings.FlushEnabled = data.Controllers.Flush != ""
	settings.ReplicaIndexDisabled = !data.ReplicaIndex
	settings.RAMQuotaMB = data.Quota.RawRAM / 1024 / 1024
	settings.ReplicaNumber = data.ReplicaNumber
	settings.BucketType = BucketType(data.BucketType)
	settings.EvictionPolicy = EvictionPolicyType(data.EvictionPolicy)
	settings.MaxTTL = time.Duration(data.MaxTTL) * time.Second
	settings.CompressionMode = CompressionMode(data.CompressionMode)
	settings.DurabilityMinLevel = DurabilityLevel(data.MinimumDurabilityLevel)
	settings.StorageBackend = StorageBackend(data.StorageBackend)

	return &settings, nil
}

type BucketSettings struct {
	MutableBucketSettings
	ConflictResolutionType ConflictResolutionType
}

func (h Management) encodeBucketSettings(posts *url.Values, opts *BucketSettings) error {
	err := h.encodeMutableBucketSettings(posts, &opts.MutableBucketSettings)
	if err != nil {
		return err
	}

	if opts.ConflictResolutionType != "" {
		posts.Add("conflictResolutionType", string(opts.ConflictResolutionType))
	}

	return nil
}

func (h Management) decodeBucketSettings(data *bucketSettingsJson) (*BucketSettings, error) {
	settings := &BucketSettings{}

	mutSettings, err := h.decodeMutableBucketSettings(data)
	if err != nil {
		return nil, err
	}

	settings.MutableBucketSettings = *mutSettings
	settings.ConflictResolutionType = ConflictResolutionType(data.ConflictResolutionType)

	return settings, nil
}

type BucketDef struct {
	Name string
	BucketSettings
}

func (h Management) decodeBucketDef(data *bucketSettingsJson) (*BucketDef, error) {
	bucket := &BucketDef{}

	settings, err := h.decodeBucketSettings(data)
	if err != nil {
		return nil, err
	}

	bucket.Name = data.Name
	bucket.BucketSettings = *settings

	return bucket, nil
}

type GetAllBucketsOptions struct {
	OnBehalfOf string
}

func (h Management) GetAllBuckets(
	ctx context.Context,
	opts *GetAllBucketsOptions,
) ([]*BucketDef, error) {
	resp, err := h.Execute(
		ctx,
		"GET",
		"/pools/default/buckets",
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	var bucketsData []bucketSettingsJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketsData)
	if err != nil {
		return nil, err
	}

	var out []*BucketDef
	for _, bucketData := range bucketsData {
		def, err := h.decodeBucketDef(&bucketData)
		if err != nil {
			return nil, err
		}

		out = append(out, def)
	}

	return out, nil
}

type GetBucketOptions struct {
	BucketName string
	OnBehalfOf string
}

func (h Management) GetBucket(
	ctx context.Context,
	opts *GetBucketOptions,
) (*BucketDef, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when updating a bucket")
	}

	resp, err := h.Execute(
		ctx,
		"GET",
		fmt.Sprintf("/pools/default/buckets/%s", opts.BucketName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	var bucketData bucketSettingsJson
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketData)
	if err != nil {
		return nil, err
	}

	def, err := h.decodeBucketDef(&bucketData)
	if err != nil {
		return nil, err
	}

	return def, nil
}

type CreateBucketOptions struct {
	BucketName string
	OnBehalfOf string
	BucketSettings
}

func (h Management) CreateBucket(
	ctx context.Context,
	opts *CreateBucketOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when creating a bucket")
	}

	posts := url.Values{}

	if opts.BucketName != "" {
		posts.Add("name", opts.BucketName)
	}

	err := h.encodeBucketSettings(&posts, &opts.BucketSettings)
	if err != nil {
		return err
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		"/pools/default/buckets",
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 202 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type UpdateBucketOptions struct {
	BucketName string
	OnBehalfOf string
	MutableBucketSettings
}

func (h Management) UpdateBucket(
	ctx context.Context,
	opts *UpdateBucketOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when updating a bucket")
	}

	posts := url.Values{}

	err := h.encodeMutableBucketSettings(&posts, &opts.MutableBucketSettings)
	if err != nil {
		return err
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s", opts.BucketName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type DeleteBucketOptions struct {
	BucketName string
	OnBehalfOf string
}

func (h Management) DeleteBucket(
	ctx context.Context,
	opts *DeleteBucketOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when deleting a bucket")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s", opts.BucketName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

type FlushBucketOptions struct {
	BucketName string
	OnBehalfOf string
}

func (h Management) FlushBucket(
	ctx context.Context,
	opts *FlushBucketOptions,
) error {
	if opts.BucketName == "" {
		return errors.New("must specify bucket name when flushing a bucket")
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", opts.BucketName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}
