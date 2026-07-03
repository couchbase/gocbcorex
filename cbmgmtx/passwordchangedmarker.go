package cbmgmtx

import (
	"encoding/json"
	"time"
)

// PasswordChangedMarker is an opaque marker representing a user's
// password_change_date at a particular point in time. It exists so that
// EnsureUser can confirm that a specific password change has propagated to
// all nodes in a cluster.
//
// The zero value represents "no password has ever been set" (i.e. it compares
// as before any real password_change_date), which makes it a safe baseline
// for newly created users.
//
// Values can only be obtained from a GetUser or UpsertUser response. This is
// deliberate: the marker must originate from the server's own clock, not the
// caller's, since comparing against a value derived from the caller's local
// wall-clock time is unsafe (see IsAfter).
type PasswordChangedMarker struct {
	t time.Time
}

// IsAfter reports whether this marker represents a point in time strictly
// after other. It's intended to be used to check whether a password change
// has propagated to a particular node, by comparing the node's current
// PasswordChangedMarker (from GetUser) against a baseline marker captured
// before the change was made (from UpsertUser).
func (m PasswordChangedMarker) IsAfter(other PasswordChangedMarker) bool {
	return m.t.After(other.t)
}

func (m PasswordChangedMarker) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.t)
}

func (m *PasswordChangedMarker) UnmarshalJSON(data []byte) error {
	var t time.Time
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	m.t = t

	return nil
}

// NewPasswordChangedMarkerForTest constructs a PasswordChangedMarker from an
// arbitrary time.Time. This exists only to support testing code that needs to
// fabricate markers, and must not be used to construct a marker from a
// caller's local clock in production code paths (use a value obtained from
// GetUser or UpsertUser instead).
func NewPasswordChangedMarkerForTest(t time.Time) PasswordChangedMarker {
	return PasswordChangedMarker{t: t}
}
