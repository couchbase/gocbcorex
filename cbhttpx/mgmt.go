package cbhttpx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type HttpManagement struct {
	HttpClient *http.Client
	UserAgent  string
	Endpoint   string
	Username   string
	Password   string
}

func (h HttpManagement) Do(ctx context.Context, method string, path string, contentType string, body io.Reader) (*http.Response, error) {
	uri := h.Endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if h.UserAgent != "" {
		req.Header.Set("User-Agent", h.UserAgent)
	}

	if h.Username != "" || h.Password != "" {
		req.SetBasicAuth(h.Username, h.Password)
	}

	return h.HttpClient.Do(req)
}

func (h HttpManagement) DecodeCommonError(resp *http.Response) error {
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

func (h HttpManagement) GetClusterConfig(ctx context.Context) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default", "", nil)
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

func (h HttpManagement) GetTerseClusterConfig(ctx context.Context) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default/nodeServices", "", nil)
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

func (h HttpManagement) StreamTerseClusterConfig(ctx context.Context) (TerseClusterConfig_Stream, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default/nodeServicesStreaming", "", nil)
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

func (h HttpManagement) GetBucketConfig(ctx context.Context, bucketName string) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Do(ctx, "GET",
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

func (h HttpManagement) GetTerseBucketConfig(ctx context.Context, bucketName string) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Do(ctx, "GET",
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

func (h HttpManagement) StreamTerseBucketConfig(ctx context.Context, bucketName string) (TerseBucketConfig_Stream, error) {
	resp, err := h.Do(ctx, "GET",
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

func (h HttpManagement) GetCollectionManifest(ctx context.Context, bucketName string) (*CollectionManifestJson, error) {
	resp, err := h.Do(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s/scopes", bucketName), "", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[CollectionManifestJson]{
		json.NewDecoder(resp.Body),
	}.Recv()
}

func (h HttpManagement) CreateScope(
	ctx context.Context,
	bucketName string,
	scopeName string,
) error {
	posts := url.Values{}
	posts.Add("name", scopeName)

	resp, err := h.Do(
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

func (h HttpManagement) DeleteScope(
	ctx context.Context,
	bucketName string,
	scopeName string,
) error {
	resp, err := h.Do(
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

func (h HttpManagement) CreateCollection(
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

	resp, err := h.Do(
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

func (h HttpManagement) DeleteCollection(
	ctx context.Context,
	bucketName string,
	scopeName string,
	collectionName string,
) error {
	resp, err := h.Do(
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
