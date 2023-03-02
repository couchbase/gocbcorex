package cbqueryx

import "time"

type QueryEarlyMetaData struct {
	Prepared string
}

type QueryMetaData struct {
	QueryEarlyMetaData

	RequestID       string
	ClientContextID string
	Status          QueryStatus
	Metrics         QueryMetrics
	Signature       interface{}
	Warnings        []QueryWarning
	Profile         interface{}
}

type QueryWarning struct {
	Code    uint32
	Message string
}

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
