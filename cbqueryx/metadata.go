package cbqueryx

import (
	"time"

	"go.uber.org/zap"
)

// QueryMetrics encapsulates various metrics gathered during a queries execution.
type QueryMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint64
	ResultSize    uint64
	MutationCount uint64
	SortCount     uint64
	ErrorCount    uint64
	WarningCount  uint64
}

func (metrics *QueryMetrics) fromData(data *jsonQueryMetrics, logger *zap.Logger) error {
	elapsedTime, err := time.ParseDuration(data.ElapsedTime)
	if err != nil {
		logger.Debug("Failed to parse query metrics elapsed time", zap.Error(err))
	}

	executionTime, err := time.ParseDuration(data.ExecutionTime)
	if err != nil {
		logger.Debug("Failed to parse query metrics execution time", zap.Error(err))
	}

	metrics.ElapsedTime = elapsedTime
	metrics.ExecutionTime = executionTime
	metrics.ResultCount = data.ResultCount
	metrics.ResultSize = data.ResultSize
	metrics.MutationCount = data.MutationCount
	metrics.SortCount = data.SortCount
	metrics.ErrorCount = data.ErrorCount
	metrics.WarningCount = data.WarningCount

	return nil
}

// QueryWarning encapsulates any warnings returned by a query.
type QueryWarning struct {
	Code    uint32
	Message string
}

func (warning *QueryWarning) fromData(data jsonQueryWarning) error {
	warning.Code = data.Code
	warning.Message = data.Message

	return nil
}

// QueryMetaData provides access to the meta-data properties of a query result.
type QueryMetaData struct {
	RequestID       string
	ClientContextID string
	Status          QueryStatus
	Metrics         QueryMetrics
	Signature       interface{}
	Warnings        []QueryWarning
	Profile         interface{}

	preparedName string
}

func (meta *QueryMetaData) fromData(data jsonQueryMetadata, logger *zap.Logger) error {
	metrics := QueryMetrics{}
	if data.Metrics != nil {
		if err := metrics.fromData(data.Metrics, logger); err != nil {
			return err
		}
	}

	warnings := make([]QueryWarning, len(data.Warnings))
	for wIdx, jsonWarning := range data.Warnings {
		err := warnings[wIdx].fromData(jsonWarning)
		if err != nil {
			return err
		}
	}

	meta.RequestID = data.RequestID
	meta.ClientContextID = data.ClientContextID
	meta.Status = data.Status
	meta.Metrics = metrics
	meta.Signature = data.Signature
	meta.Warnings = warnings
	meta.Profile = data.Profile
	meta.preparedName = data.Prepared

	return nil
}

type jsonQueryMetadata struct {
	RequestID       string             `json:"requestID"`
	ClientContextID string             `json:"clientContextID"`
	Status          QueryStatus        `json:"status"`
	Warnings        []jsonQueryWarning `json:"warnings"`
	Metrics         *jsonQueryMetrics  `json:"metrics,omitempty"`
	Profile         interface{}        `json:"profile"`
	Signature       interface{}        `json:"signature"`
	Prepared        string             `json:"prepared"`
}

type jsonQueryMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   uint64 `json:"resultCount"`
	ResultSize    uint64 `json:"resultSize"`
	MutationCount uint64 `json:"mutationCount,omitempty"`
	SortCount     uint64 `json:"sortCount,omitempty"`
	ErrorCount    uint64 `json:"errorCount,omitempty"`
	WarningCount  uint64 `json:"warningCount,omitempty"`
}

type jsonQueryWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}
