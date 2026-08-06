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
type PasswordChangedMarker struct {
	t time.Time
}

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
