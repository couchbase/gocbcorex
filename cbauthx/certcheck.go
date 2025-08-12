package cbauthx

import (
	"context"
	"crypto/tls"
)

type CertCheck interface {
	CheckCertificate(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error)
}
