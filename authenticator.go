package gocbcorex

import (
	"crypto/tls"
)

type Authenticator interface {
	GetClientCertificate(service ServiceType, hostPort string) (*tls.Certificate, error)
	GetCredentials(service ServiceType, hostPort string) (string, string, error)
}

type PasswordAuthenticator struct {
	Username string
	Password string
}

func (a *PasswordAuthenticator) GetClientCertificate(
	service ServiceType, hostPort string,
) (*tls.Certificate, error) {
	return nil, nil
}

func (a *PasswordAuthenticator) GetCredentials(
	service ServiceType, hostPort string,
) (string, string, error) {
	return a.Username, a.Password, nil
}

type kvClientAuth struct {
	Authenticator Authenticator
}

var _ KvClientAuth = (*kvClientAuth)(nil)

func (v *kvClientAuth) GetAuth(address string) (username, password string, clientCert *tls.Certificate, err error) {
	clientCert, err = v.Authenticator.GetClientCertificate(ServiceTypeMemd, address)
	if err != nil {
		return "", "", nil, err
	}

	username, password, err = v.Authenticator.GetCredentials(ServiceTypeMemd, address)
	if err != nil {
		return "", "", nil, err
	}

	return username, password, clientCert, nil
}
