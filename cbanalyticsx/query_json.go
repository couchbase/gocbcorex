package cbanalyticsx

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

type queryMetaDataJson struct {
	RequestID       string              `json:"requestID,omitempty"`
	ClientContextID string              `json:"clientContextID,omitempty"`
	Status          Status              `json:"status,omitempty"`
	Warnings        []*queryWarningJson `json:"warnings,omitempty"`
	Metrics         *queryMetricsJson   `json:"metrics,omitempty"`
	Signature       json.RawMessage     `json:"signature,omitempty"`
}

type queryMetricsJson struct {
	ElapsedTime      string `json:"elapsedTime,omitempty"`
	ExecutionTime    string `json:"executionTime,omitempty"`
	ResultCount      uint64 `json:"resultCount,omitempty"`
	ResultSize       uint64 `json:"resultSize,omitempty"`
	MutationCount    uint64 `json:"mutationCount,omitempty"`
	SortCount        uint64 `json:"sortCount,omitempty"`
	ErrorCount       uint64 `json:"errorCount,omitempty"`
	WarningCount     uint64 `json:"warningCount,omitempty"`
	ProcessedObjects uint64 `json:"processedObjects,omitempty"`
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
