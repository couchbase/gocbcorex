package cbqueryx

import (
	"encoding/json"
	"strings"
)

func EncodeIdentifier(identifier string) string {
	out := identifier
	out = strings.ReplaceAll(out, "\\", "\\\\")
	out = strings.ReplaceAll(out, "`", "\\`")
	return "`" + out + "`"
}

func EncodeValue(value interface{}) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
