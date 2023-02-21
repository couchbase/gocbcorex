package gocbcorex

import "crypto/tls"

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
