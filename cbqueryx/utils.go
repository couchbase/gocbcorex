package cbqueryx

import (
	"encoding/json"
	"strings"
)

func EncodeIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "\\`") + "`"
}

func EncodeValue(value interface{}) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
