package transactionsx

import (
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/stretchr/testify/assert"
)

func TestOboUsersEqual(t *testing.T) {
	local := &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "local"}
	localCopy := &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "local"}
	external := &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "external"}

	assert.True(t, oboUsersEqual(nil, nil))
	assert.True(t, oboUsersEqual(local, localCopy))
	assert.False(t, oboUsersEqual(local, nil))
	assert.False(t, oboUsersEqual(nil, local))
	// Same username in different domains is a different identity.
	assert.False(t, oboUsersEqual(local, external))
}
