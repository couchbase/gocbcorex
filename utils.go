package gocbcorex

import (
	"net"
	"net/url"
	"strings"

	"golang.org/x/exp/slices"
)

func getHostFromUri(uri string) (string, error) {
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

func filterStringsOut(strs []string, toRemove []string) []string {
	out := make([]string, 0, len(strs))
	for _, str := range strs {
		if !slices.Contains(toRemove, str) {
			out = append(out, str)
		}
	}
	return out
}
