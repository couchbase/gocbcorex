package cbmgmtx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
)

var (
	ErrMetaKvEntryNotFound = errors.New("metakv entry not found")
	ErrMetaKvConflict      = errors.New("metakv conflict")
	ErrMetaKvNotEmpty      = errors.New("metakv directory not empty")
	ErrMetaKvTimeout       = errors.New("metakv timeout")
	ErrMetaKvWrongType     = errors.New("metakv wrong type")
	ErrMetaKvNotChanged    = errors.New("metakv not changed")
	ErrMetaKvExists        = errors.New("metakv exists")
)

type metaKv2ErrorResponse struct {
	Message  string `json:"message"`
	Path     string `json:"path"`
	Revision string `json:"revision"`
}

func decodeMetaKv2Error(resp *http.Response) error {
	bodyBytes, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return contextualError{
			Description: "failed to read error body for non-success response",
			Cause:       readErr,
		}
	}

	var errResp metaKv2ErrorResponse
	isMetaKvResp := json.Unmarshal(bodyBytes, &errResp) == nil && errResp.Message != ""

	var err error
	msgLower := strings.ToLower(errResp.Message)

	if !isMetaKvResp && (resp.StatusCode == 404 || resp.StatusCode == 405) {
		// The metakv2 endpoints were only introduced in server 8.0.0, older servers have
		// no handler registered for these paths and reply with a generic non-json error
		// rather than one of the structured metakv errors below.  Which error you get
		// depends on both the server version and the method used, 7.6.x replies with a
		// 404 for everything, whereas 7.2.x replies with a 405 for PUT and DELETE.
		err = ErrUnsupportedFeature
	} else if strings.Contains(msgLower, "not found") || resp.StatusCode == 404 {
		err = ErrMetaKvEntryNotFound
	} else if strings.Contains(msgLower, "conflict") || resp.StatusCode == 409 {
		err = ErrMetaKvConflict
	} else if strings.Contains(msgLower, "not empty") {
		err = ErrMetaKvNotEmpty
	} else if strings.Contains(msgLower, "timeout") || resp.StatusCode == 504 {
		err = ErrMetaKvTimeout
	} else if strings.Contains(msgLower, "wrong type") {
		err = ErrMetaKvWrongType
	} else if strings.Contains(msgLower, "not changed") {
		err = ErrMetaKvNotChanged
	} else if strings.Contains(msgLower, "exists") {
		err = ErrMetaKvExists
	} else if resp.StatusCode == 401 || resp.StatusCode == 403 {
		err = ErrAccessDenied
	} else if resp.StatusCode == 400 {
		err = ErrServerInvalidArg
	}

	if err == nil {
		err = fmt.Errorf("metakv error (%d): %s", resp.StatusCode, string(bodyBytes))
	}

	return ServerError{
		Cause:      err,
		StatusCode: resp.StatusCode,
		Body:       bodyBytes,
	}
}

func normalizeMetaKv2Path(path string) string {
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

type MetaKv2Entry struct {
	Path     string
	Revision string
	Value    []byte
	IsDir    bool
}

type GetMetaKv2Options struct {
	Path       string
	Recursive  bool
	Depth      int
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type GetMetaKv2Response struct {
	Revision string
	Value    []byte                  // Populated for leaf key queries
	Entries  map[string]MetaKv2Entry // Populated for directory queries (path -> entry)
}

type metaKv2GetLeafResponse struct {
	Revision string `json:"revision"`
	Value    string `json:"value"`
}

type metaKv2RawDirNode struct {
	Revision string          `json:"revision"`
	Value    json.RawMessage `json:"value"`
}

func parseMetaKv2Directory(raw json.RawMessage, results map[string]MetaKv2Entry) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var nodeMap map[string]metaKv2RawDirNode
	if err := json.Unmarshal(raw, &nodeMap); err != nil {
		return err
	}

	for path, node := range nodeMap {
		if strings.HasSuffix(path, "/") {
			results[path] = MetaKv2Entry{
				Path:     path,
				Revision: node.Revision,
				IsDir:    true,
			}
			if len(node.Value) > 0 && string(node.Value) != "null" {
				if err := parseMetaKv2Directory(node.Value, results); err != nil {
					return err
				}
			}
		} else {
			var valStr string
			if len(node.Value) > 0 && string(node.Value) != "null" {
				if err := json.Unmarshal(node.Value, &valStr); err != nil {
					return err
				}
			}
			results[path] = MetaKv2Entry{
				Path:     path,
				Revision: node.Revision,
				Value:    []byte(valStr),
				IsDir:    false,
			}
		}
	}
	return nil
}

func (h Management) GetMetaKv2(ctx context.Context, opts *GetMetaKv2Options) (*GetMetaKv2Response, error) {
	if opts.Path == "" {
		return nil, errors.New("must specify path when getting metakv2 key")
	}

	normPath := normalizeMetaKv2Path(opts.Path)
	reqUrl := "/_metakv2" + normPath

	values := url.Values{}
	if opts.Recursive {
		values.Set("recursive", "true")
	}
	if opts.Depth > 0 {
		values.Set("depth", strconv.Itoa(opts.Depth))
	}
	if len(values) > 0 {
		reqUrl += "?" + values.Encode()
	}

	resp, err := h.Execute(ctx, "GET", reqUrl, "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, decodeMetaKv2Error(resp)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(normPath, "/") {
		var rawResp struct {
			Revision string          `json:"revision"`
			Value    json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(bodyBytes, &rawResp); err != nil {
			return nil, err
		}

		entries := make(map[string]MetaKv2Entry)
		if err := parseMetaKv2Directory(rawResp.Value, entries); err != nil {
			return nil, err
		}

		return &GetMetaKv2Response{
			Revision: rawResp.Revision,
			Entries:  entries,
		}, nil
	}

	var leafResp metaKv2GetLeafResponse
	if err := json.Unmarshal(bodyBytes, &leafResp); err != nil {
		return nil, err
	}

	return &GetMetaKv2Response{
		Revision: leafResp.Revision,
		Value:    []byte(leafResp.Value),
	}, nil
}

type PutMetaKv2Options struct {
	Path       string
	Value      []byte
	Recursive  bool
	Create     bool
	Revision   string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type PutMetaKv2Response struct {
	Revision string
}

type metaKv2MutationResponse struct {
	Message  string `json:"message"`
	Revision string `json:"revision"`
	Path     string `json:"path"`
}

func (h Management) PutMetaKv2(ctx context.Context, opts *PutMetaKv2Options) (*PutMetaKv2Response, error) {
	if opts.Path == "" {
		return nil, errors.New("must specify path when putting metakv2 key")
	}

	normPath := normalizeMetaKv2Path(opts.Path)
	reqUrl := "/_metakv2" + normPath

	values := url.Values{}
	if opts.Recursive {
		values.Set("recursive", "true")
	}
	if opts.Create {
		values.Set("create", "true")
	}
	if opts.Revision != "" {
		values.Set("rev", opts.Revision)
	}
	if len(values) > 0 {
		reqUrl += "?" + values.Encode()
	}

	var body io.Reader
	var contentType string
	if !strings.HasSuffix(normPath, "/") && opts.Value != nil {
		body = bytes.NewReader(opts.Value)
	}

	resp, err := h.Execute(ctx, "PUT", reqUrl, contentType, opts.OnBehalfOf, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, decodeMetaKv2Error(resp)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var mutResp metaKv2MutationResponse
	if err := json.Unmarshal(bodyBytes, &mutResp); err != nil {
		return nil, err
	}

	if mutResp.Message == "Exists" {
		return nil, ServerError{
			Cause:      ErrMetaKvExists,
			StatusCode: resp.StatusCode,
			Body:       bodyBytes,
		}
	}
	if mutResp.Message == "Not Changed" {
		return nil, ServerError{
			Cause:      ErrMetaKvNotChanged,
			StatusCode: resp.StatusCode,
			Body:       bodyBytes,
		}
	}

	return &PutMetaKv2Response{
		Revision: mutResp.Revision,
	}, nil
}

type DeleteMetaKv2Options struct {
	Path       string
	Recursive  bool
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type DeleteMetaKv2Response struct {
	Revision string
}

func (h Management) DeleteMetaKv2(ctx context.Context, opts *DeleteMetaKv2Options) (*DeleteMetaKv2Response, error) {
	if opts.Path == "" {
		return nil, errors.New("must specify path when deleting metakv2 key")
	}

	normPath := normalizeMetaKv2Path(opts.Path)
	reqUrl := "/_metakv2" + normPath

	if opts.Recursive {
		reqUrl += "?recursive=true"
	}

	resp, err := h.Execute(ctx, "DELETE", reqUrl, "", opts.OnBehalfOf, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, decodeMetaKv2Error(resp)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var mutResp metaKv2MutationResponse
	if err := json.Unmarshal(bodyBytes, &mutResp); err != nil {
		return nil, err
	}

	return &DeleteMetaKv2Response{
		Revision: mutResp.Revision,
	}, nil
}

type GetMetaKv2SnapshotOptions struct {
	Keys       []string
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type GetMetaKv2SnapshotResponse struct {
	Revision string
	Entries  map[string]MetaKv2Entry
}

type metaKv2SnapshotResponse struct {
	Revision string `json:"revision"`
	Value    map[string]struct {
		Revision string `json:"revision"`
		Value    string `json:"value"`
	} `json:"value"`
}

func (h Management) GetMetaKv2Snapshot(ctx context.Context, opts *GetMetaKv2SnapshotOptions) (*GetMetaKv2SnapshotResponse, error) {
	if len(opts.Keys) == 0 {
		return nil, errors.New("must specify at least one key when fetching metakv2 snapshot")
	}

	normKeys := make([]string, len(opts.Keys))
	for i, k := range opts.Keys {
		normKeys[i] = normalizeMetaKv2Path(k)
	}

	reqBody, err := json.Marshal(normKeys)
	if err != nil {
		return nil, err
	}

	resp, err := h.Execute(ctx, "POST", "/_metakv2/_controller/getSnapshot", "application/json", opts.OnBehalfOf, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return nil, decodeMetaKv2Error(resp)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var snapResp metaKv2SnapshotResponse
	if err := json.Unmarshal(bodyBytes, &snapResp); err != nil {
		return nil, err
	}

	entries := make(map[string]MetaKv2Entry)
	for k, v := range snapResp.Value {
		entries[k] = MetaKv2Entry{
			Path:     k,
			Revision: v.Revision,
			Value:    []byte(v.Value),
			IsDir:    false,
		}
	}

	return &GetMetaKv2SnapshotResponse{
		Revision: snapResp.Revision,
		Entries:  entries,
	}, nil
}

type MetaKv2SetEntry struct {
	Value    string `json:"value"`
	Revision string `json:"revision,omitempty"`
	Create   bool   `json:"create,omitempty"`
}

type SetMetaKv2MultipleOptions struct {
	Entries    map[string]MetaKv2SetEntry
	Recursive  bool
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

type SetMetaKv2MultipleResponse struct {
	Revision string
}

func (h Management) SetMetaKv2Multiple(ctx context.Context, opts *SetMetaKv2MultipleOptions) (*SetMetaKv2MultipleResponse, error) {
	if len(opts.Entries) == 0 {
		return nil, errors.New("must specify at least one entry when setting multiple metakv2 keys")
	}

	normEntries := make(map[string]MetaKv2SetEntry, len(opts.Entries))
	for k, e := range opts.Entries {
		normEntries[normalizeMetaKv2Path(k)] = e
	}

	reqBody, err := json.Marshal(normEntries)
	if err != nil {
		return nil, err
	}

	reqUrl := "/_metakv2/_controller/setMultiple"
	if opts.Recursive {
		reqUrl += "?recursive=true"
	}

	resp, err := h.Execute(ctx, "POST", reqUrl, "application/json", opts.OnBehalfOf, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, decodeMetaKv2Error(resp)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var mutResp metaKv2MutationResponse
	if err := json.Unmarshal(bodyBytes, &mutResp); err != nil {
		return nil, err
	}

	if mutResp.Message == "Not Changed" {
		return nil, ServerError{
			Cause:      ErrMetaKvNotChanged,
			StatusCode: resp.StatusCode,
			Body:       bodyBytes,
		}
	}

	return &SetMetaKv2MultipleResponse{
		Revision: mutResp.Revision,
	}, nil
}

type SyncMetaKv2QuorumOptions struct {
	Timeout    time.Duration
	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (h Management) SyncMetaKv2Quorum(ctx context.Context, opts *SyncMetaKv2QuorumOptions) error {
	reqUrl := "/_metakv2/_controller/syncQuorum"
	if opts.Timeout > 0 {
		reqUrl += fmt.Sprintf("?timeout=%d", opts.Timeout.Milliseconds())
	}

	resp, err := h.Execute(ctx, "POST", reqUrl, "", opts.OnBehalfOf, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		return decodeMetaKv2Error(resp)
	}

	return nil
}
