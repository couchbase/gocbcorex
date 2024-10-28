package cbconfig

import (
	"bytes"
	"encoding/json"
)

func ParseTerseConfig(config []byte, sourceHostname string) (*TerseConfigJson, error) {
	config = bytes.ReplaceAll(config, []byte("$HOST"), []byte(sourceHostname))
	var configOut *TerseConfigJson
	err := json.Unmarshal(config, &configOut)
	if err != nil {
		return nil, err
	}
	return configOut, nil
}
