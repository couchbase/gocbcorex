package cbhttpx

import (
	"encoding/json"
	"io"
)

func ReadAsJsonAndClose[T any](r io.ReadCloser) (T, error) {
	var resp T
	jsonDec := json.NewDecoder(r)
	err := jsonDec.Decode(&resp)
	if err != nil {
		var emptyResp T
		_ = r.Close()
		return emptyResp, err
	}

	_ = r.Close()
	return resp, nil
}
