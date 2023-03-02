package cbqueryx

import "encoding/json"

type QueryStatus string

const (
	QueryStatusRunning   QueryStatus = "running"
	QueryStatusSuccess   QueryStatus = "success"
	QueryStatusErrors    QueryStatus = "errors"
	QueryStatusCompleted QueryStatus = "completed"
	QueryStatusStopped   QueryStatus = "stopped"
	QueryStatusTimeout   QueryStatus = "timeout"
	QueryStatusClosed    QueryStatus = "closed"
	QueryStatusFatal     QueryStatus = "fatal"
	QueryStatusAborted   QueryStatus = "aborted"
	QueryStatusUnknown   QueryStatus = "unknown"
)

type queryErrorResponseJson struct {
	Errors []*queryErrorJson `json:"errors,omitempty"`
}

type queryEarlyMetaDataJson struct {
	Prepared string `json:"prepared,omitempty"`
}

type queryMetaDataJson struct {
	queryEarlyMetaDataJson
	RequestID       string              `json:"requestID,omitempty"`
	ClientContextID string              `json:"clientContextID,omitempty"`
	Status          QueryStatus         `json:"status,omitempty"`
	Errors          []*queryErrorJson   `json:"errors,omitempty"`
	Warnings        []*queryWarningJson `json:"warnings,omitempty"`
	Metrics         *queryMetricsJson   `json:"metrics,omitempty"`
	Profile         json.RawMessage     `json:"profile,omitempty"`
	Signature       json.RawMessage     `json:"signature,omitempty"`
}

type queryMetricsJson struct {
	ElapsedTime   string `json:"elapsedTime,omitempty"`
	ExecutionTime string `json:"executionTime,omitempty"`
	ResultCount   uint64 `json:"resultCount,omitempty"`
	ResultSize    uint64 `json:"resultSize,omitempty"`
	MutationCount uint64 `json:"mutationCount,omitempty"`
	SortCount     uint64 `json:"sortCount,omitempty"`
	ErrorCount    uint64 `json:"errorCount,omitempty"`
	WarningCount  uint64 `json:"warningCount,omitempty"`
}

type queryWarningJson struct {
	Code    uint32 `json:"code,omitempty"`
	Message string `json:"msg,omitempty"`
}

type queryErrorJson struct {
	Code   uint32                 `json:"code,omitempty"`
	Msg    string                 `json:"msg,omitempty"`
	Reason map[string]interface{} `json:"reason,omitempty"`
	Retry  bool                   `json:"retry,omitempty"`
}
