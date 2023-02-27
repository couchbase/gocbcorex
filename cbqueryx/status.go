package cbqueryx

// QueryStatus provides information about the current status of a query.
type QueryStatus string

const (
	// QueryStatusRunning indicates the query is still running
	QueryStatusRunning QueryStatus = "running"

	// QueryStatusSuccess indicates the query was successful.
	QueryStatusSuccess QueryStatus = "success"

	// QueryStatusErrors indicates a query completed with errors.
	QueryStatusErrors QueryStatus = "errors"

	// QueryStatusCompleted indicates a query has completed.
	QueryStatusCompleted QueryStatus = "completed"

	// QueryStatusStopped indicates a query has been stopped.
	QueryStatusStopped QueryStatus = "stopped"

	// QueryStatusTimeout indicates a query timed out.
	QueryStatusTimeout QueryStatus = "timeout"

	// QueryStatusClosed indicates that a query was closed.
	QueryStatusClosed QueryStatus = "closed"

	// QueryStatusFatal indicates that a query ended with a fatal error.
	QueryStatusFatal QueryStatus = "fatal"

	// QueryStatusAborted indicates that a query was aborted.
	QueryStatusAborted QueryStatus = "aborted"

	// QueryStatusUnknown indicates that the query status is unknown.
	QueryStatusUnknown QueryStatus = "unknown"
)
