package cbauthx

import (
	"context"
	"crypto/x509"
)

type CertCheck interface {
	CheckCertificate(ctx context.Context, clientCert *x509.Certificate) (UserInfo, error)
}
