package gocbcorex

import "net/url"

func getHostFromUri(uri string) (string, error) {
	// TODO(brett19): This is probably going to exist in a lot of places, utils?
	parsedUrl, err := url.Parse(uri)
	if err != nil {
		return "", err
	}

	return parsedUrl.Host, nil
}
