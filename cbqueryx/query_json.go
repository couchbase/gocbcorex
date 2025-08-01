package cbqueryx

import "encoding/json"

type Status string

const (
	StatusRunning   Status = "running"
	StatusSuccess   Status = "success"
	StatusErrors    Status = "errors"
	StatusCompleted Status = "completed"
	StatusStopped   Status = "stopped"
	StatusTimeout   Status = "timeout"
	StatusClosed    Status = "closed"
	StatusFatal     Status = "fatal"
	StatusAborted   Status = "aborted"
	StatusUnknown   Status = "unknown"
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
	Status          Status              `json:"status,omitempty"`
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
	Code   uint32                `json:"code,omitempty"`
	Msg    string                `json:"msg,omitempty"`
	Reason *queryErrorReasonJson `json:"reason,omitempty"`
	Retry  bool                  `json:"retry,omitempty"`
}

type queryErrorReasonJson struct {
	Code    uint32 `json:"code,omitempty"`
	Key     string `json:"key,omitempty"`
	Message string `json:"message,omitempty"`
}
