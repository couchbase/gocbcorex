package cbhttpx

import "encoding/json"

type JsonBlockStreamer[T any] struct {
	Decoder *json.Decoder
}

func (s JsonBlockStreamer[T]) Recv() (*T, error) {
	var block T
	err := s.Decoder.Decode(&block)
	if err != nil {
		return nil, err
	}
	return &block, nil
}
