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

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type Management struct {
	Transport  http.RoundTripper
	UserAgent  string
	Endpoint   string
	Username   string
	Password   string
	OnBehalfOf string
}

func (h Management) NewRequest(
	ctx context.Context,
	method string, path string,
	contentType string, body io.Reader,
) (*http.Request, error) {
	return cbhttpx.RequestBuilder{
		UserAgent:     h.UserAgent,
		Endpoint:      h.Endpoint,
		BasicAuthUser: h.Username,
		BasicAuthPass: h.Password,
		CbOnBehalfOf:  h.OnBehalfOf,
	}.NewRequest(ctx, method, path, contentType, body)
}

func (h Management) Execute(ctx context.Context, method string, path string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := h.NewRequest(ctx, method, path, contentType, body)
	if err != nil {
		return nil, err
	}

	return cbhttpx.Client{
		Transport: h.Transport,
	}.Do(req)
}

func (h Management) DecodeCommonError(resp *http.Response) error {
	if resp.StatusCode == 404 {
		return ServerError{
			Cause:      ErrUnsupportedFeature,
			StatusCode: resp.StatusCode,
		}
	} else if resp.StatusCode == 401 {
		return ServerError{
			Cause:      ErrAccessDenied,
			StatusCode: resp.StatusCode,
		}
	}

	return ServerError{
		Cause:      errors.New("unexpected response status"),
		StatusCode: resp.StatusCode,
	}
}

type GetClusterConfigOptions struct {
}

func (h Management) GetClusterConfig(ctx context.Context, opts *GetClusterConfigOptions) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default", "", nil)
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
}

func (h Management) GetTerseClusterConfig(ctx context.Context, opts *GetTerseClusterConfigOptions) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default/nodeServices", "", nil)
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
}

type TerseClusterConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseClusterConfig(ctx context.Context, opts *StreamTerseClusterConfigOptions) (TerseClusterConfig_Stream, error) {
	resp, err := h.Execute(ctx, "GET", "/pools/default/nodeServicesStreaming", "", nil)
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
}

func (h Management) GetBucketConfig(ctx context.Context, opts *GetBucketConfigOptions) (*cbconfig.FullConfigJson, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when fetching a bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s", opts.BucketName), "", nil)
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
}

func (h Management) GetTerseBucketConfig(ctx context.Context, opts *GetTerseBucketConfigOptions) (*cbconfig.TerseConfigJson, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when fetching a terse bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/b/%s", opts.BucketName), "", nil)
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
}

type TerseBucketConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseBucketConfig(ctx context.Context, opts *StreamTerseBucketConfigOptions) (TerseBucketConfig_Stream, error) {
	if opts.BucketName == "" {
		return nil, errors.New("must specify bucket name when streaming a bucket config")
	}

	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/bs/%s", opts.BucketName), "", nil)
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
		fmt.Sprintf("/pools/default/buckets/%s/scopes", opts.BucketName), "", nil)
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
		"application/x-www-form-urlencoded", strings.NewReader(posts.Encode()))
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
		"", nil)
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
	MaxExpiry      uint64
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
	if opts != nil {
		if opts.MaxExpiry > 0 {
			posts.Add("maxTTL", fmt.Sprintf("%d", int(opts.MaxExpiry)))
		}
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", opts.BucketName, opts.ScopeName),
		"application/x-www-form-urlencoded", strings.NewReader(posts.Encode()))
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
		"", nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}
