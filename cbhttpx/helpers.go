package cbhttpx

import "encoding/json"

type httpJsonBlockStreamer[T any] struct {
	dec *json.Decoder
}

func (s httpJsonBlockStreamer[T]) Recv() (*T, error) {
	var block T
	err := s.dec.Decode(&block)
	if err != nil {
		return nil, err
	}
	return &block, nil
}
