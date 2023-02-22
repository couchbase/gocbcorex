package gocbcorex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
)

type HttpManagement struct {
	HttpClient *http.Client
	UserAgent  string
	Endpoint   string
	Username   string
	Password   string
}

func (h HttpManagement) Do(ctx context.Context, method string, path string, body io.Reader) (*http.Response, error) {
	uri := h.Endpoint + path
	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, err
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
	// TODO(brett19): Add better handling of these http error messages

	if resp.StatusCode == 404 {
		return errors.New("unsupported feature")
	} else if resp.StatusCode == 401 {
		return errors.New("no access")
	}

	return errors.New("unexpected response status")
}

func (h HttpManagement) ClusterConfig(ctx context.Context) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[cbconfig.FullConfigJson]{
		json.NewDecoder(resp.Body),
	}.Recv()
}

func (h HttpManagement) TerseClusterConfig(ctx context.Context) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default/nodeServices", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[cbconfig.TerseConfigJson]{
		json.NewDecoder(resp.Body),
	}.Recv()
}

type TerseClusterConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h HttpManagement) StreamTerseClusterConfig(ctx context.Context) (TerseClusterConfig_Stream, error) {
	resp, err := h.Do(ctx, "GET", "/pools/default/nodeServicesStreaming", nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return &httpJsonBlockStreamer[cbconfig.TerseConfigJson]{
		json.NewDecoder(resp.Body),
	}, nil
}

func (h HttpManagement) BucketConfig(ctx context.Context, bucketName string) (*cbconfig.FullConfigJson, error) {
	resp, err := h.Do(ctx, "GET",
		fmt.Sprintf("/pools/default/buckets/%s/", bucketName), nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[cbconfig.FullConfigJson]{
		json.NewDecoder(resp.Body),
	}.Recv()
}

func (h HttpManagement) TerseBucketConfig(ctx context.Context, bucketName string) (*cbconfig.TerseConfigJson, error) {
	resp, err := h.Do(ctx, "GET",
		fmt.Sprintf("/pools/default/b/%s/", bucketName), nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[cbconfig.TerseConfigJson]{
		json.NewDecoder(resp.Body),
	}.Recv()
}

type TerseBucketConfig_Stream interface {
	Recv() (*cbconfig.TerseConfigJson, error)
}

func (h HttpManagement) StreamTerseBucketConfig(ctx context.Context, bucketName string) (TerseBucketConfig_Stream, error) {
	resp, err := h.Do(ctx, "GET",
		fmt.Sprintf("/pools/default/bs/%s/", bucketName), nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, h.DecodeCommonError(resp)
	}

	return httpJsonBlockStreamer[cbconfig.TerseConfigJson]{
		json.NewDecoder(resp.Body),
	}, nil
}
