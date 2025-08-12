package cbauthx

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCertCheckCache_NoCacheExternal(t *testing.T) {
	certChecks := 0

	localCert := generateTestCert()
	localConn := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{localCert},
	}

	adminCert := generateTestCert()
	adminConn := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{adminCert},
	}

	externalCert := generateTestCert()
	externalConn := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{externalCert},
	}

	invalidCert := generateTestCert()
	invalidConn := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{invalidCert},
	}

	checker := NewCertCheckCached(&CertCheckMock{
		CheckCertificateFunc: func(ctx context.Context, connState *tls.ConnectionState) (UserInfo, error) {
			certChecks++

			switch connState.PeerCertificates[0] {
			case localCert:
				return UserInfo{Domain: "local"}, nil
			case adminCert:
				return UserInfo{Domain: "admin"}, nil
			case externalCert:
				return UserInfo{Domain: "external"}, nil
			default:
				return UserInfo{}, ErrInvalidAuth
			}
		},
	})

	// check that 'local' domain is cached
	info, err := checker.CheckCertificate(context.Background(), localConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "local")
	assert.Equal(t, 1, certChecks)

	info, err = checker.CheckCertificate(context.Background(), localConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "local")
	assert.Equal(t, 1, certChecks)

	// check that 'admin' domain is cached
	info, err = checker.CheckCertificate(context.Background(), adminConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "admin")
	assert.Equal(t, 2, certChecks)

	info, err = checker.CheckCertificate(context.Background(), adminConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "admin")
	assert.Equal(t, 2, certChecks)

	// check that 'external' cert is not cached
	info, err = checker.CheckCertificate(context.Background(), externalConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "external")
	assert.Equal(t, 3, certChecks)

	info, err = checker.CheckCertificate(context.Background(), externalConn)
	require.NoError(t, err)
	assert.Equal(t, info.Domain, "external")
	assert.Equal(t, 4, certChecks)

	// check that invaild cert is not cached
	info, err = checker.CheckCertificate(context.Background(), invalidConn)
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 5, certChecks)

	info, err = checker.CheckCertificate(context.Background(), invalidConn)
	require.ErrorIs(t, err, ErrInvalidAuth)
	assert.Equal(t, 6, certChecks)
}

func generateTestCert() *x509.Certificate {
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},

		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(7 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		panic(err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		panic(err)
	}

	return cert
}
