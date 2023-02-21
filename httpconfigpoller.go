package gocbcorex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"go.uber.org/zap"
)

type configStreamBlock struct {
	Bytes []byte
}

func (i *configStreamBlock) UnmarshalJSON(data []byte) error {
	i.Bytes = make([]byte, len(data))
	copy(i.Bytes, data)
	return nil
}

func hostnameFromURI(uri string) string {
	uriInfo, err := url.Parse(uri)
	if err != nil {
		return uri
	}

	hostname, err := hostFromHostPort(uriInfo.Host)
	if err != nil {
		return uri
	}

	return hostname
}

type httpConfigPoller struct {
	logger *zap.Logger

	confHTTPRetryDelay   time.Duration
	confHTTPRedialPeriod time.Duration
	confHTTPMaxWait      time.Duration
	httpClient           HTTPClientManager
	bucketName           string
	seenNodes            map[string]uint64

	shutdownSig chan struct{}
}

type httpPollerProperties struct {
	Logger               *zap.Logger
	ConfHTTPRetryDelay   time.Duration
	ConfHTTPRedialPeriod time.Duration
	ConfHTTPMaxWait      time.Duration
	HTTPClient           HTTPClientManager
	BucketName           string
}

func newhttpConfigPoller(props httpPollerProperties) *httpConfigPoller {
	return &httpConfigPoller{
		logger:               loggerOrNop(props.Logger),
		confHTTPRedialPeriod: props.ConfHTTPRedialPeriod,
		confHTTPRetryDelay:   props.ConfHTTPRetryDelay,
		confHTTPMaxWait:      props.ConfHTTPMaxWait,
		httpClient:           props.HTTPClient,
		bucketName:           props.BucketName,
		seenNodes:            make(map[string]uint64),
		shutdownSig:          make(chan struct{}),
	}
}

func (hcc *httpConfigPoller) Close() error {
	// We don't just close the sig so that we can wait on it to be read, signalling that the poller
	// has stopped.
	hcc.shutdownSig <- struct{}{}

	return nil
}

func (hcc *httpConfigPoller) Watch() (<-chan *TerseConfigJsonWithSource, error) {
	waitPeriod := hcc.confHTTPRetryDelay
	maxConnPeriod := hcc.confHTTPRedialPeriod

	var iterNum uint64 = 1
	iterSawConfig := false

	outputCh := make(chan *TerseConfigJsonWithSource)
	hcc.logger.Info("http config looper starting")
	go func() {
		for {
			hcc.logger.Debug("starting config poll iteration")

			var path string
			if hcc.bucketName == "" {
				path = "/pools/default/nodeServicesStreaming"
			} else {
				path = fmt.Sprintf("/pools/default/bs/%s", hcc.bucketName)
			}

			select {
			case <-hcc.shutdownSig:
				return
			default:
			}

			client, err := hcc.httpClient.GetClient()
			if err != nil {
				hcc.logger.Warn("failed to get http client", zap.Error(err))
				continue
			}

			endpoints := client.ManagementEndpoints()
			pickedSrv := hcc.pickEndpoint(iterNum, endpoints)

			if pickedSrv == "" {
				hcc.logger.Warn("failed to pick a server")
				// All servers have been visited during this iteration

				if !iterSawConfig {
					hcc.logger.Debug("waiting for config")
					// Wait for a period before trying again if there was a problem...
					// We also watch for the client being shut down.
					select {
					case <-hcc.shutdownSig:
						return
					case <-time.After(waitPeriod):
					}
				}

				// Go to next iteration and try all servers again
				iterNum++
				iterSawConfig = false
				continue
			}

			hcc.logger.Debug("selected server to poll", zap.String("server", pickedSrv))
			hostname := hostnameFromURI(pickedSrv)

			var resp *HTTPResponse
			// 1 on success, 0 on failure for node, -1 for generic failure
			var doConfigRequest func(bool) int

			closeBody := func(failMsg string) {
				err := resp.Raw.Body.Close()
				if err != nil {
					hcc.logger.Debug(failMsg, zap.Error(err))
				}
			}

			doConfigRequest = func(is2x bool) int {
				// HTTP request time!
				hcc.logger.Debug("requesting config",
					zap.String("server", pickedSrv),
					zap.String("uri", path))

				req := &HTTPRequest{
					Endpoint: pickedSrv,
					Path:     path,
					Method:   "GET",
				}

				resp, err = client.Do(context.Background(), req)
				if err != nil {
					hcc.logger.Warn("http request failed", zap.Error(err))
					return 0
				}

				if resp.Raw.StatusCode != 200 {
					err := resp.Raw.Body.Close()
					if err != nil {
						hcc.logger.Warn("failed to close failed response body", zap.Error(err))
					}

					if resp.Raw.StatusCode == 401 {
						hcc.logger.Warn("failed to fetch config, bad auth")
						return -1
					} else if resp.Raw.StatusCode == 404 {
						if is2x {
							hcc.logger.Warn("Failed to fetch config, bad bucket")
							return -1
						}

						return doConfigRequest(true)
					}

					hcc.logger.Warn("failed to fetch config, unexpected status code",
						zap.Int("statusCode", resp.Raw.StatusCode))
					return 0
				}
				return 1
			}

			switch doConfigRequest(false) {
			case 0:
				continue
			case -1:
				continue
			}

			hcc.logger.Debug("connected")

			var autoDisconnected int32

			// Autodisconnect eventually
			go func() {
				select {
				case <-time.After(maxConnPeriod):
				case <-hcc.shutdownSig:
				}

				hcc.logger.Debug("automatically resetting http connection")

				atomic.StoreInt32(&autoDisconnected, 1)

				closeBody("auto-dc failed to close response body")
			}()

			dec := json.NewDecoder(resp.Raw.Body)
			configBlock := new(configStreamBlock)
			for {
				err := dec.Decode(configBlock)
				if err != nil {
					if atomic.LoadInt32(&autoDisconnected) == 1 {
						// If we know we intentionally disconnected, we know we do not
						// need to close the client, nor log an error, since this was
						// expected behaviour
						break
					}

					hcc.logger.Warn("failed to decode config block", zap.Error(err))

					if err != io.EOF {
						closeBody("failed to close response body after decode error")
					}

					break
				}

				//hcc.logger.Warn("received config block", zap.ByteString("config", configBlock.Bytes))

				bkCfg, err := parseConfig(configBlock.Bytes, hostname)
				if err != nil {
					hcc.logger.Warn("failed to parse config", zap.Error(err))

					break
				}

				hcc.logger.Debug("new config received successfully")

				iterSawConfig = true
				outputCh <- bkCfg
			}
		}
	}()

	return outputCh, nil
}

func (hcc *httpConfigPoller) pickEndpoint(iterNum uint64, endpoints []string) string {
	var pickedSrv string

	for _, srv := range endpoints {
		if hcc.seenNodes[srv] >= iterNum {
			continue
		}
		pickedSrv = srv
		break
	}

	if pickedSrv != "" {
		hcc.seenNodes[pickedSrv] = iterNum
	}

	return pickedSrv
}

func parseConfig(config []byte, srcHost string) (*TerseConfigJsonWithSource, error) {
	configStr := strings.Replace(string(config), "$HOST", srcHost, -1)

	bk := new(cbconfig.TerseConfigJson)
	err := json.Unmarshal([]byte(configStr), bk)
	if err != nil {
		return nil, err
	}

	return &TerseConfigJsonWithSource{
		Config:         bk,
		SourceHostname: srcHost,
	}, nil
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
