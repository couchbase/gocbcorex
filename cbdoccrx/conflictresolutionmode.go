package cbdoccrx

type ConflictResolutionMode string

const (
	ConflictResolutionModeSeqNo ConflictResolutionMode = "seqno"
	ConflictResolutionModeLww   ConflictResolutionMode = "lww"
)
