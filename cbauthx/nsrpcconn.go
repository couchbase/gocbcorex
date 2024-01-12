package cbauthx

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
)

type NsRpcConn struct {
	netConn net.Conn

	*bufio.Reader
	io.WriteCloser
}

var _ io.ReadWriteCloser = (*NsRpcConn)(nil)

type NsRpcConnOptions struct {
	TlsConfig *tls.Config
	UserAgent string
	Username  string
	Password  string
}

func DialNsRpcConn(ctx context.Context, uri string, opts *NsRpcConnOptions) (*NsRpcConn, error) {
	if opts == nil {
		opts = &NsRpcConnOptions{}
	}

	req, err := http.NewRequest("RPCCONNECT", uri, nil)
	if err != nil {
		return nil, err
	}

	if opts.UserAgent != "" {
		req.Header.Set("User-Agent", opts.UserAgent)
	}

	if opts.Username != "" || opts.Password != "" {
		req.SetBasicAuth(opts.Username, opts.Password)
	}

	dialer := net.Dialer{}
	tcpConn, err := dialer.DialContext(ctx, "tcp", req.URL.Host)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to dial",
			Cause:   err,
		}
	}

	// attempt to set no-delay, if it fails, ignore it
	_ = tcpConn.(*net.TCPConn).SetNoDelay(true)

	var netConn net.Conn
	if req.URL.Scheme == "https" {
		tlsConfig := &tls.Config{}
		if opts.TlsConfig != nil {
			tlsConfig = opts.TlsConfig.Clone()
		}

		if tlsConfig.ServerName == "" {
			tlsConfig.ServerName = req.URL.Hostname()
		}

		netConn = tls.Client(tcpConn, tlsConfig)
	} else {
		netConn = tcpConn
	}

	netRdr := bufio.NewReader(netConn)

	err = req.Write(netConn)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to write request to server",
			Cause:   err,
		}
	}

	resp, err := http.ReadResponse(netRdr, req)
	if err != nil {
		return nil, &contextualError{
			Message: "failed to read response from server",
			Cause:   err,
		}
	}

	if resp.StatusCode != 200 {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, &contextualError{
				Message: "failed to read error body for non-success response",
				Cause:   readErr,
			}
		}

		return nil, &ServerError{
			StatusCode: resp.StatusCode,
			Body:       bodyBytes,
		}
	}

	return &NsRpcConn{
		netConn:     netConn,
		Reader:      netRdr,
		WriteCloser: netConn,
	}, nil
}
