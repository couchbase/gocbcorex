package gocbcorex

import (
	"net"
	"net/url"
	"strings"
)

func getHostFromUri(uri string) (string, error) {
	// TODO(brett19): This is probably going to exist in a lot of places, utils?
	parsedUrl, err := url.Parse(uri)
	if err != nil {
		return "", err
	}

	return parsedUrl.Host, nil
}

func hostFromHostPort(hostport string) (string, error) {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", err
	}

	// If this is an IPv6 address, we need to rewrap it in []
	if strings.Contains(host, ":") {
		return "[" + host + "]", nil
	}

	return host, nil
}
