package cbdoccrx

type ConflictResolutionMode string

const (
	ConflictResolutionModeTimestamp ConflictResolutionMode = "timestamp"
	ConflictResolutionModeLww       ConflictResolutionMode = "lww"
)
