package cbmgmtx

import (
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
