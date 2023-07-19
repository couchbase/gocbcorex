package cbsearchx

import "encoding/json"

type searchIndexRespJson struct {
	Status   string           `json:"status"`
	IndexDef *searchIndexJson `json:"indexDef"`
}

type searchIndexDefsJson struct {
	IndexDefs   map[string]searchIndexJson `json:"indexDefs"`
	ImplVersion string                     `json:"implVersion"`
}

type searchIndexesRespJson struct {
	Status    string              `json:"status"`
	IndexDefs searchIndexDefsJson `json:"indexDefs"`
}

type searchIndexJson struct {
	UUID         string                     `json:"uuid"`
	Name         string                     `json:"name"`
	SourceName   string                     `json:"sourceName"`
	Type         string                     `json:"type"`
	Params       map[string]json.RawMessage `json:"params"`
	SourceUUID   string                     `json:"sourceUUID"`
	SourceParams map[string]json.RawMessage `json:"sourceParams"`
	SourceType   string                     `json:"sourceType"`
	PlanParams   map[string]json.RawMessage `json:"planParams"`
}

type analyzeDocumentJson struct {
	Status   string          `json:"status"`
	Analyzed json.RawMessage `json:"analyzed"`
}

type indexedDocumentsJson struct {
	Count uint64 `json:"count"`
}
