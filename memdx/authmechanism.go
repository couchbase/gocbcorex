package memdx

// AuthMechanism represents a type of auth that can be performed.
type AuthMechanism string

const (
	// PlainAuthMechanism represents that PLAIN auth should be performed.
	PlainAuthMechanism = AuthMechanism("PLAIN")

	// ScramSha1AuthMechanism represents that SCRAM SHA1 auth should be performed.
	ScramSha1AuthMechanism = AuthMechanism("SCRAM-SHA1")

	// ScramSha256AuthMechanism represents that SCRAM SHA256 auth should be performed.
	ScramSha256AuthMechanism = AuthMechanism("SCRAM-SHA256")

	// ScramSha512AuthMechanism represents that SCRAM SHA512 auth should be performed.
	ScramSha512AuthMechanism = AuthMechanism("SCRAM-SHA512")
)
