package cbhttpx

import (
	"bytes"
	"encoding/json"
	"net"
	"net/url"
)

type httpJsonBlockStreamer[T any] struct {
	Decoder *json.Decoder
}

func (s httpJsonBlockStreamer[T]) Recv() (*T, error) {
	var block T
	err := s.Decoder.Decode(&block)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func getHostFromEndpoint(endpoint string) (string, error) {
	parsedUrl, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	hostname, _, _ := net.SplitHostPort(parsedUrl.Host)
	if hostname == "" {
		hostname = parsedUrl.Host
	}

	return hostname, nil
}

type httpConfigJsonBlockStreamer[T any] struct {
	Decoder  *json.Decoder
	Endpoint string
}

func (s httpConfigJsonBlockStreamer[T]) Recv() (*T, error) {
	var rawBlock json.RawMessage
	err := s.Decoder.Decode(&rawBlock)
	if err != nil {
		return nil, err
	}

	host, err := getHostFromEndpoint(s.Endpoint)
	if err != nil {
		return nil, err
	}
	rawBlock = bytes.ReplaceAll(rawBlock, []byte("$HOST"), []byte(host))

	var block T
	err = json.Unmarshal(rawBlock, &block)
	if err != nil {
		return nil, err
	}

	return &block, nil
}
