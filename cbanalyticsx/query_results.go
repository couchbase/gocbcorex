package cbanalyticsx

import "time"

type MetaData struct {
	RequestID       string
	ClientContextID string
	Status          Status
	Metrics         Metrics
	Signature       interface{}
	Warnings        []Warning
	Profile         interface{}
}

type Warning struct {
	Code    uint32
	Message string
}

type Metrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint64
	ResultSize       uint64
	MutationCount    uint64
	SortCount        uint64
	ErrorCount       uint64
	WarningCount     uint64
	ProcessedObjects uint64
}
