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

func (h Management) GetClusterConfig(ctx context.Context) (*cbconfig.FullConfigJson, error) {
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

func (h Management) GetTerseClusterConfig(ctx context.Context) (*cbconfig.TerseConfigJson, error) {
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

type TerseClusterConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseClusterConfig(ctx context.Context) (TerseClusterConfig_Stream, error) {
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

func (h Management) GetBucketConfig(ctx context.Context, bucketName string) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s", bucketName), "", nil)
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

func (h Management) GetTerseBucketConfig(ctx context.Context, bucketName string) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/b/%s", bucketName), "", nil)
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

type TerseBucketConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h Management) StreamTerseBucketConfig(ctx context.Context, bucketName string) (TerseBucketConfig_Stream, error) {
	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/bs/%s", bucketName), "", nil)
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

func (h Management) GetCollectionManifest(ctx context.Context, bucketName string) (*CollectionManifestJson, error) {
	resp, err := h.Execute(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", bucketName), "", nil)
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

func (h Management) CreateScope(
	ctx context.Context,
	bucketName string,
	scopeName string,
) error {
	posts := url.Values{}
	posts.Add("name", scopeName)

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", bucketName),
		"application/x-www-form-urlencoded", strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

func (h Management) DeleteScope(
	ctx context.Context,
	bucketName string,
	scopeName string,
) error {
	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", bucketName, scopeName),
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
	MaxExpiry uint64
}

func (h Management) CreateCollection(
	ctx context.Context,
	bucketName string,
	scopeName string,
	collectionName string,
	opts *CreateCollectionOptions,
) error {
	posts := url.Values{}
	posts.Add("name", collectionName)
	if opts != nil {
		if opts.MaxExpiry > 0 {
			posts.Add("maxTTL", fmt.Sprintf("%d", int(opts.MaxExpiry)))
		}
	}

	resp, err := h.Execute(
		ctx,
		"POST",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", bucketName, scopeName),
		"application/x-www-form-urlencoded", strings.NewReader(posts.Encode()))
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}

func (h Management) DeleteCollection(
	ctx context.Context,
	bucketName string,
	scopeName string,
	collectionName string,
) error {
	resp, err := h.Execute(
		ctx,
		"DELETE",
		fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", bucketName, scopeName, collectionName),
		"", nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return h.DecodeCommonError(resp)
	}

	return nil
}
