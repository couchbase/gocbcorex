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
	"github.com/google/go-querystring/query"
)

type Management struct {
	Transport http.RoundTripper
	UserAgent string
	Endpoint  string
	Auth      cbhttpx.Authenticator
}

func (h Management) NewRequest(
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

func (h Management) Execute(
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

func (h Management) DecodeCommonError(resp *http.Response) error {
	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return contextualError{
			Description: "failed to read error body for non-success response",
			Cause:       readErr,
		}
	}

	fieldNameMap := map[string]string{
		`durability_min_level`: "DurabilityMinLevel",
		`ramquota`:             "RamQuotaMB",
		`replicanumber`:        "ReplicaNumber",
		`maxttl`:               "MaxTTL",
		`history`:              "HistoryEnabled",
	}

	var err error
	errText := strings.ToLower(string(bodyBytes))

	if strings.Contains(errText, "not found") && strings.Contains(errText, "collection") {
		err = ErrCollectionNotFound
	} else if strings.Contains(errText, "not found") && strings.Contains(errText, "scope") {
		err = ErrScopeNotFound
	} else if strings.Contains(errText, "not found") && strings.Contains(errText, "bucket") {
		err = ErrBucketNotFound
	} else if strings.Contains(errText, "not found") && strings.Contains(errText, "user") {
		err = ErrUserNotFound
	} else if strings.Contains(errText, "unknown user") {
		// 8.0.0 now returns "unknown user" rather than "not found" for user not found.
		err = ErrUserNotFound
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "collection") {
		err = ErrCollectionExists
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "scope") {
		err = ErrScopeExists
	} else if strings.Contains(errText, "already exists") && strings.Contains(errText, "bucket") {
		err = ErrBucketExists
	} else if strings.Contains(errText, "flush is disabled") {
		err = ErrFlushDisabled
	} else if strings.Contains(errText, "requested resource not found") {
		// BUG(MB-60488): if we get this specific error, and its none of the above errors, then this
		// indicates that it was the top level resource which could not be found, which is the bucket.
		err = ErrBucketNotFound
	} else if strings.Contains(errText, "non existent bucket") {
		// BUG(MB-60487): with some builds of server 7.6.0, rather than the normal kinds of not-found
		// errors, we receive "Attempt to access non existent bucket" instead.
		err = ErrBucketNotFound
	} else if strings.Contains(errText, "not yet complete, but will continue") {
		err = ErrOperationDelayed
	} else if strings.Contains(errText, "unexpected server error") {
		err = ErrUnexpectedServerError
	} else if resp.StatusCode == 400 {
		sErr := parseForInvalidArg(errText)
		var ok bool
		if sErr.Argument, ok = fieldNameMap[sErr.Argument]; ok {
			err = sErr
		} else if strings.Contains(errText, "not allowed on this type of bucket") {
			err = &ServerInvalidArgError{
				Argument: "HistoryEnabled",
				Reason:   errText,
			}
		}
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

func parseForInvalidArg(body string) *ServerInvalidArgError {
	sErr := ServerInvalidArgError{}
	invArg := struct {
		Errors map[string]string `json:"errors"`
	}{}
	err := json.Unmarshal([]byte(body), &invArg)
	if err != nil {
		return &sErr
	}

	for k, v := range invArg.Errors {
		sErr.Argument = k
		sErr.Reason = v
		break
	}

	return &sErr
}

type GetClusterConfigOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) GetClusterConfig(ctx context.Context, opts *GetClusterConfigOptions) (*cbconfig.FullClusterConfigJson, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.FullClusterConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type StreamFullClusterConfigOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type FullClusterConfig_Stream interface {
	Recv() (*cbconfig.FullClusterConfigJson, error)
}

func (h Management) StreamFullClusterConfig(ctx context.Context, opts *StreamFullClusterConfigOptions) (FullClusterConfig_Stream, error) {
	resp, err := h.Execute(ctx, "GET", "/poolsStreaming/default", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpConfigJsonBlockStreamer[cbconfig.FullClusterConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}, nil
}

type GetTerseClusterConfigOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) GetBucketConfig(ctx context.Context, opts *GetBucketConfigOptions) (*cbconfig.FullBucketConfigJson, error) {
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

	return httpConfigJsonBlockStreamer[cbconfig.FullBucketConfigJson]{
		Decoder:  json.NewDecoder(resp.Body),
		Endpoint: h.Endpoint,
	}.Recv()
}

type GetTerseBucketConfigOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

type CheckBucketExistsOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) CheckBucketExists(ctx context.Context, opts *CheckBucketExistsOptions) (bool, error) {
	if opts.BucketName == "" {
		return false, errors.New("must specify bucket name when checking a bucket exists")
	}

	resp, err := h.Execute(ctx, "HEAD",
		fmt.Sprintf("/pools/default/b/%s", opts.BucketName), "", opts.OnBehalfOf, nil)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		err := h.DecodeCommonError(resp)
		if errors.Is(err, ErrBucketNotFound) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

type StreamTerseBucketConfigOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

type GetClusterInfoOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type ClusterInfoResponse struct {
	IsAdminCreds          bool              `json:"isAdminCreds"`
	IsRoAdminCreds        bool              `json:"isROAdminCreds"`
	IsEnterprise          bool              `json:"isEnterprise"`
	ConfigProfile         string            `json:"configProfile"`
	AllowedServices       []string          `json:"allowedServices"`
	IsDeveloperPreview    bool              `json:"isDeveloperPreview"`
	PackageVariant        string            `json:"packageVariant"`
	Uuid                  string            `json:"uuid"`
	ImplementationVersion string            `json:"implementationVersion"`
	ComponentsVersion     map[string]string `json:"componentsVersion"`
	// Pools
	// Settings
}

func (h Management) GetClusterInfo(ctx context.Context, opts *GetClusterInfoOptions) (*ClusterInfoResponse, error) {
	resp, err := h.Execute(ctx, "GET", "/pools", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return cbhttpx.JsonBlockStreamer[ClusterInfoResponse]{
		Decoder: json.NewDecoder(resp.Body),
	}.Recv()
}

type GetTerseClusterInfoOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type TerseClusterInfoResponse struct {
	ClusterUUID          string `json:"clusterUUID"`
	Orchestrator         string `json:"orchestrator"`
	IsBalanced           bool   `json:"isBalanced"`
	ClusterCompatVersion string `json:"clusterCompatVersion"`
}

func (h Management) GetTerseClusterInfo(ctx context.Context, opts *GetTerseClusterConfigOptions) (*TerseClusterInfoResponse, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default/terseClusterInfo", "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return cbhttpx.JsonBlockStreamer[TerseClusterInfoResponse]{
		Decoder: json.NewDecoder(resp.Body),
	}.Recv()
}

type GetCollectionManifestOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) GetCollectionManifest(ctx context.Context, opts *GetCollectionManifestOptions) (*cbconfig.CollectionManifestJson, error) {
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

	return cbhttpx.JsonBlockStreamer[cbconfig.CollectionManifestJson]{
		Decoder: json.NewDecoder(resp.Body),
	}.Recv()
}

type CreateScopeOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type CreateScopeResponse struct {
	ManifestUid string
}

type manifestUidJSON struct {
	ManifestUid string `json:"uid"`
}

func (h Management) CreateScope(
	ctx context.Context,
	opts *CreateScopeOptions,
) (*CreateScopeResponse, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when creating a scope")
	}
	if opts.ScopeName == "" {
		return nil, errors.New("must specify scope name when creating a scope")
	}

	posts := url.Values{}
	posts.Add("name", opts.ScopeName)

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", opts.BucketName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	createResp, err := cbhttpx.ReadAsJsonAndClose[manifestUidJSON](resp.Body)
	if err != nil {
		return nil, err
	}

	return &CreateScopeResponse{
		ManifestUid: createResp.ManifestUid,
	}, nil
}

type DeleteScopeOptions struct {
	BucketName string
	ScopeName  string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type DeleteScopeResponse struct {
	ManifestUid string
}

func (h Management) DeleteScope(
	ctx context.Context,
	opts *DeleteScopeOptions,
) (*DeleteScopeResponse, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when deleting a scope")
	}
	if opts.ScopeName == "" {
		return nil, errors.New("must specify scope name when deleting a scope")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", opts.BucketName, opts.ScopeName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	deleteResp, err := cbhttpx.ReadAsJsonAndClose[manifestUidJSON](resp.Body)
	if err != nil {
		return nil, err
	}

	return &DeleteScopeResponse{
		ManifestUid: deleteResp.ManifestUid,
	}, nil
}

type CreateCollectionOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	MaxTTL         int32
	HistoryEnabled *bool
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

type CreateCollectionResponse struct {
	ManifestUid string
}

func (h Management) CreateCollection(
	ctx context.Context,
	opts *CreateCollectionOptions,
) (*CreateCollectionResponse, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when creating a collection")
	}
	if opts.ScopeName == "" {
		return nil, errors.New("must specify scope name when creating a collection")
	}
	if opts.CollectionName == "" {
		return nil, errors.New("must specify collection name when creating a collection")
	}

	posts := url.Values{}
	posts.Add("name", opts.CollectionName)

	if opts.MaxTTL != 0 {
		posts.Add("maxTTL", fmt.Sprintf("%d", int(opts.MaxTTL)))
	}

	if opts.HistoryEnabled != nil {
		posts.Add("history", fmt.Sprintf("%t", *opts.HistoryEnabled))
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", opts.BucketName, opts.ScopeName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	createResp, err := cbhttpx.ReadAsJsonAndClose[manifestUidJSON](resp.Body)
	if err != nil {
		return nil, err
	}

	return &CreateCollectionResponse{
		ManifestUid: createResp.ManifestUid,
	}, nil
}

type DeleteCollectionOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
}

type DeleteCollectionResponse struct {
	ManifestUid string
}

func (h Management) DeleteCollection(
	ctx context.Context,
	opts *DeleteCollectionOptions,
) (*DeleteCollectionResponse, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when deleting a collection")
	}
	if opts.ScopeName == "" {
		return nil, errors.New("must specify scope name when deleting a collection")
	}
	if opts.CollectionName == "" {
		return nil, errors.New("must specify collection name when deleting a collection")
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", opts.BucketName, opts.ScopeName, opts.CollectionName),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	deleteResp, err := cbhttpx.ReadAsJsonAndClose[manifestUidJSON](resp.Body)
	if err != nil {
		return nil, err
	}

	return &DeleteCollectionResponse{
		ManifestUid: deleteResp.ManifestUid,
	}, nil
}

type UpdateCollectionOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	OnBehalfOf     *cbhttpx.OnBehalfOfInfo
	HistoryEnabled *bool
	MaxTTL         *int32
}

type UpdateCollectionResponse struct {
	ManifestUid string
}

func (h Management) UpdateCollection(
	ctx context.Context,
	opts *UpdateCollectionOptions,
) (*UpdateCollectionResponse, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when updating a collection")
	}
	if opts.ScopeName == "" {
		return nil, errors.New("must specify scope name when updating a collection")
	}
	if opts.CollectionName == "" {
		return nil, errors.New("must specify collection name when updating a collection")
	}

	posts := url.Values{}
	if opts.MaxTTL != nil {
		posts.Add("maxTTL", fmt.Sprintf("%d", *opts.MaxTTL))
	}

	if opts.HistoryEnabled != nil {
		posts.Add("history", fmt.Sprintf("%t", *opts.HistoryEnabled))
	}

	resp, err := h.Execute(
		ctx,
		"PATCH",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", opts.BucketName, opts.ScopeName, opts.CollectionName),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	updateResp, err := cbhttpx.ReadAsJsonAndClose[manifestUidJSON](resp.Body)
	if err != nil {
		return nil, err
	}

	return &UpdateCollectionResponse{
		ManifestUid: updateResp.ManifestUid,
	}, nil
}

type MutableBucketSettings struct {
	FlushEnabled                      bool
	RAMQuotaMB                        uint64
	ReplicaNumber                     uint32
	EvictionPolicy                    EvictionPolicyType
	MaxTTL                            time.Duration
	CompressionMode                   CompressionMode
	DurabilityMinLevel                DurabilityLevel
	HistoryRetentionCollectionDefault *bool
	HistoryRetentionBytes             uint64
	HistoryRetentionSeconds           uint32
}

func (h Management) encodeMutableBucketSettings(posts *url.Values, opts *MutableBucketSettings) error {
	if opts.FlushEnabled {
		posts.Add("flushEnabled", "1")
	} else {
		posts.Add("flushEnabled", "0")
	}
	if opts.RAMQuotaMB > 0 {
		posts.Add("ramQuotaMB", fmt.Sprintf("%d", opts.RAMQuotaMB))
	}
	if opts.ReplicaNumber > 0 {
		posts.Add("replicaNumber", fmt.Sprintf("%d", opts.ReplicaNumber))
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
	if opts.HistoryRetentionBytes > 0 {
		posts.Add("historyRetentionBytes", fmt.Sprintf("%d", opts.HistoryRetentionBytes))
	}
	if opts.HistoryRetentionSeconds > 0 {
		posts.Add("historyRetentionSeconds", fmt.Sprintf("%d", opts.HistoryRetentionSeconds))
	}
	if opts.HistoryRetentionCollectionDefault != nil {
		if *opts.HistoryRetentionCollectionDefault {
			posts.Add("historyRetentionCollectionDefault", "true")
		} else {
			posts.Add("historyRetentionCollectionDefault", "false")
		}
	}

	return nil
}

func (h Management) decodeMutableBucketSettings(data *cbconfig.FullBucketConfigJson) (*MutableBucketSettings, error) {
	settings := MutableBucketSettings{}

	settings.FlushEnabled = data.Controllers.Flush != ""
	settings.RAMQuotaMB = data.Quota.RawRAM / 1024 / 1024
	settings.ReplicaNumber = data.ReplicaNumber
	settings.EvictionPolicy = EvictionPolicyType(data.EvictionPolicy)
	settings.MaxTTL = time.Duration(data.MaxTTL) * time.Second
	settings.CompressionMode = CompressionMode(data.CompressionMode)
	settings.DurabilityMinLevel = DurabilityLevel(data.MinimumDurabilityLevel)
	settings.HistoryRetentionCollectionDefault = data.HistoryRetentionCollectionDefault
	settings.HistoryRetentionBytes = data.HistoryRetentionBytes
	settings.HistoryRetentionSeconds = data.HistoryRetentionSeconds

	return &settings, nil
}

type BucketSettings struct {
	MutableBucketSettings
	ConflictResolutionType ConflictResolutionType
	ReplicaIndex           bool
	BucketType             BucketType
	StorageBackend         StorageBackend
}

func (h Management) encodeBucketSettings(posts *url.Values, opts *BucketSettings) error {
	err := h.encodeMutableBucketSettings(posts, &opts.MutableBucketSettings)
	if err != nil {
		return err
	}

	if opts.ConflictResolutionType != "" {
		posts.Add("conflictResolutionType", string(opts.ConflictResolutionType))
	}
	if opts.BucketType != BucketTypeUnset {
		posts.Add("bucketType", string(opts.BucketType))
	}
	if opts.ReplicaIndex {
		if opts.BucketType == BucketTypeEphemeral {
			return errors.New("cannot specify ReplicaIndex for Ephemeral buckets")
		}
		posts.Add("replicaIndex", "1")
	} else if opts.BucketType != BucketTypeEphemeral {
		posts.Add("replicaIndex", "0")
	}
	if opts.StorageBackend != "" {
		posts.Add("storageBackend", string(opts.StorageBackend))
	}

	return nil
}

func (h Management) decodeBucketSettings(data *cbconfig.FullBucketConfigJson) (*BucketSettings, error) {
	settings := &BucketSettings{}

	mutSettings, err := h.decodeMutableBucketSettings(data)
	if err != nil {
		return nil, err
	}

	settings.MutableBucketSettings = *mutSettings
	settings.ConflictResolutionType = ConflictResolutionType(data.ConflictResolutionType)
	settings.ReplicaIndex = data.ReplicaIndex
	settings.BucketType = BucketType(data.BucketType)
	settings.StorageBackend = StorageBackend(data.StorageBackend)

	return settings, nil
}

type BucketDef struct {
	Name string
	UUID string
	BucketSettings

	RawConfig *cbconfig.FullBucketConfigJson
}

func (h Management) decodeBucketDef(data *cbconfig.FullBucketConfigJson) (*BucketDef, error) {
	bucket := &BucketDef{}

	settings, err := h.decodeBucketSettings(data)
	if err != nil {
		return nil, err
	}

	bucket.UUID = data.UUID
	bucket.Name = data.Name
	bucket.BucketSettings = *settings
	bucket.RawConfig = data

	return bucket, nil
}

type GetAllBucketsOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

	bucketsData, err := cbhttpx.ReadAsJsonAndClose[[]cbconfig.FullBucketConfigJson](resp.Body)
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
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

	bucketData, err := cbhttpx.ReadAsJsonAndClose[cbconfig.FullBucketConfigJson](resp.Body)
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
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

	_ = resp.Body.Close()
	return nil
}

type UpdateBucketOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
	MutableBucketSettings
	StorageBackend StorageBackend
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

	_ = resp.Body.Close()
	return nil
}

type DeleteBucketOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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
		err := h.DecodeCommonError(resp)

		// A delayed operation is considered a success for deletion, since
		// bucket management is already eventually consistent anyways.
		if errors.Is(err, ErrOperationDelayed) {
			return nil
		}

		return err
	}

	_ = resp.Body.Close()
	return nil
}

type FlushBucketOptions struct {
	BucketName string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
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

	_ = resp.Body.Close()
	return nil
}

type GetAutoFailoverSettingsResponse_FailoverOnDataDiskIssues struct {
	Enabled        bool `json:"enabled"`
	TimePeriodSecs int  `json:"timePeriod"`
}

type GetAutoFailoverSettingsResponse struct {
	Enabled                            bool                                                     `json:"enabled"`
	Timeout                            int                                                      `json:"timeout"`
	MaxCount                           int                                                      `json:"maxCount"`
	FailoverOnDataDiskIssues           GetAutoFailoverSettingsResponse_FailoverOnDataDiskIssues `json:"failoverOnDataDiskIssues"`
	CanAbortRebalance                  bool                                                     `json:"canAbortRebalance"`
	FailoverPreserveDurabilityMajority bool                                                     `json:"failoverPreserveDurabilityMajority"`
}

type GetAutoFailoverSettingsRequest struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) GetAutoFailoverSettings(
	ctx context.Context,
	opts *GetAutoFailoverSettingsRequest,
) (*GetAutoFailoverSettingsResponse, error) {
	resp, err := h.Execute(
		ctx,
		"GET",
		"/settings/autoFailover",
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	settings, err := cbhttpx.ReadAsJsonAndClose[*GetAutoFailoverSettingsResponse](resp.Body)
	if err != nil {
		return nil, err
	}

	return settings, nil
}

type ConfigureAutoFailoverRequest_FailoverOnDataDiskIssues struct {
	Enabled        bool `url:"enabled,omitempty"`
	TimePeriodSecs int  `url:"timePeriod,omitempty"`
}

type ConfigureAutoFailoverRequest struct {
	Enabled                            *bool                                                  `url:"enabled,omitempty"`
	Timeout                            *int                                                   `url:"timeout,omitempty"`
	MaxCount                           *int                                                   `url:"maxCount,omitempty"`
	FailoverOnDataDiskIssues           *ConfigureAutoFailoverRequest_FailoverOnDataDiskIssues `url:"failoverOnDataDiskIssues,omitempty"`
	CanAbortRebalance                  *bool                                                  `url:"canAbortRebalance,omitempty"`
	FailoverPreserveDurabilityMajority *bool                                                  `url:"failoverPreserveDurabilityMajority,omitempty"`
	OnBehalfOf                         *cbhttpx.OnBehalfOfInfo                                `url:"-"`
}

func (h Management) ConfigureAutoFailover(ctx context.Context, req *ConfigureAutoFailoverRequest) error {
	form, err := query.Values(req)
	if err != nil {
		return err
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		"/settings/autoFailover",
		"application/x-www-form-urlencoded", req.OnBehalfOf, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	_ = resp.Body.Close()

	return nil
}

// AuthDomain specifies the user domain of a specific user
type AuthDomain string

const (
	// AuthDomainAdmin specifies users that are locally stored as Administrators.
	AuthDomainAdmin AuthDomain = "admin"

	// AuthDomainLocal specifies users that are locally stored in Couchbase.
	AuthDomainLocal AuthDomain = "local"

	// AuthDomainExternal specifies users that are externally stored
	// (in LDAP for instance).
	AuthDomainExternal AuthDomain = "external"
)

type OriginJson struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type RoleJson struct {
	RoleName       string `json:"role"`
	BucketName     string `json:"bucket_name"`
	ScopeName      string `json:"scope_name"`
	CollectionName string `json:"collection_name"`
}

type RoleWithOriginsJson struct {
	RoleJson

	Origins []OriginJson
}

type UserJson struct {
	ID              string                `json:"id"`
	Name            string                `json:"name"`
	Roles           []RoleWithOriginsJson `json:"roles"`
	Groups          []string              `json:"groups"`
	Domain          AuthDomain            `json:"domain"`
	ExternalGroups  []string              `json:"external_groups"`
	PasswordChanged time.Time             `json:"password_change_date"`
}

type GetAllUsersOptions struct {
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) GetAllUsers(
	ctx context.Context,
	opts *GetAllUsersOptions,
) ([]*UserJson, error) {
	resp, err := h.Execute(
		ctx,
		"GET",
		"/settings/rbac/users",
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	usersData, err := cbhttpx.ReadAsJsonAndClose[[]*UserJson](resp.Body)
	if err != nil {
		return nil, err
	}

	return usersData, nil
}

type UpsertUserOptions struct {
	Domain      AuthDomain
	Username    string
	DisplayName string
	Password    string
	Roles       []string
	Groups      []string
	OnBehalfOf  *cbhttpx.OnBehalfOfInfo
}

func (h Management) UpsertUser(
	ctx context.Context,
	opts *UpsertUserOptions,
) error {
	if opts.Username == "" {
		return errors.New("must specify username when upserting a user")
	}

	if opts.Domain == "" {
		opts.Domain = "local"
	}

	posts := url.Values{}

	if opts.DisplayName != "" {
		posts.Add("name", opts.DisplayName)
	}
	if opts.Password != "" {
		posts.Add("password", opts.Password)
	}
	if len(opts.Groups) > 0 {
		posts.Add("groups", strings.Join(opts.Groups, ","))
	}
	if len(opts.Roles) > 0 {
		posts.Add("roles", strings.Join(opts.Roles, ","))
	}

	resp, err := h.Execute(
		ctx,
		"PUT",
		fmt.Sprintf("/settings/rbac/users/%s/%s", opts.Domain, opts.Username),
		"application/x-www-form-urlencoded", opts.OnBehalfOf, strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	_ = resp.Body.Close()
	return nil
}

type DeleteUserOptions struct {
	Domain     AuthDomain
	Username   string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) DeleteUser(
	ctx context.Context,
	opts *DeleteUserOptions,
) error {
	if opts.Username == "" {
		return errors.New("must specify username when deleting a user")
	}

	if opts.Domain == "" {
		opts.Domain = "local"
	}

	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/settings/rbac/users/%s/%s", opts.Domain, opts.Username),
		"", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	_ = resp.Body.Close()
	return nil
}
