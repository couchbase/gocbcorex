package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
	"github.com/couchbase/stellar-nebula/utils/latestonlychannel"
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
	confHTTPRetryDelay   time.Duration
	confHTTPRedialPeriod time.Duration
	confHTTPMaxWait      time.Duration
	httpClient           *http.Client
	endpoints            routeEndpoints
	bucketName           string
	seenNodes            map[string]uint64
}

type httpPollerProperties struct {
	ConfHTTPRetryDelay   time.Duration
	ConfHTTPRedialPeriod time.Duration
	ConfHTTPMaxWait      time.Duration
	HttpClient           *http.Client // TODO: when a http component exists, use it.
	BucketName           string
}

func newhttpConfigPoller(endpoints routeEndpoints, props httpPollerProperties) *httpConfigPoller {
	return &httpConfigPoller{
		confHTTPRedialPeriod: props.ConfHTTPRedialPeriod,
		confHTTPRetryDelay:   props.ConfHTTPRetryDelay,
		confHTTPMaxWait:      props.ConfHTTPMaxWait,
		httpClient:           props.HttpClient,
		endpoints:            endpoints,
		bucketName:           props.BucketName,
		seenNodes:            make(map[string]uint64),
	}
}

func (hcc *httpConfigPoller) Watch(ctx context.Context) (<-chan *TerseConfigJsonWithSource, error) {
	waitPeriod := hcc.confHTTPRetryDelay
	maxConnPeriod := hcc.confHTTPRedialPeriod

	var iterNum uint64 = 1
	iterSawConfig := false

	outputCh := make(chan *TerseConfigJsonWithSource)
	log.Printf("HTTP Looper starting.")
	go func() {
		for {
			var path string
			if hcc.bucketName == "" {
				path = "/pools/default/nodeServices"
			} else {
				path = fmt.Sprintf("/pools/default/bs/%s", hcc.bucketName)
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			pickedSrv := hcc.pickEndpoint(iterNum)

			if pickedSrv == "" {
				log.Printf("Pick Failed.")
				// All servers have been visited during this iteration

				if !iterSawConfig {
					log.Printf("Looper waiting...")
					// Wait for a period before trying again if there was a problem...
					// We also watch for the client being shut down.
					select {
					case <-ctx.Done():
						return
					case <-time.After(waitPeriod):
					}
				}
				log.Printf("Looping again.")
				// Go to next iteration and try all servers again
				iterNum++
				iterSawConfig = false
				continue
			}

			log.Printf("Http Picked: %s.", pickedSrv)

			hostname := hostnameFromURI(pickedSrv)
			log.Printf("HTTP Hostname: %s.", hostname)

			var resp *http.Response
			// 1 on success, 0 on failure for node, -1 for generic failure
			var doConfigRequest func(bool) int

			doConfigRequest = func(is2x bool) int {
				streamPath := "bs"
				if is2x {
					streamPath = "bucketsStreaming"
				}
				// HTTP request time!
				uri := fmt.Sprintf("/pools/default/%s/%s", streamPath, url.PathEscape(hcc.bucketName))
				log.Printf("Requesting config from: %s/%s.", pickedSrv, uri)

				req, err := http.NewRequestWithContext(ctx, "GET", hostname+path, nil)
				if err != nil {
					log.Printf("Failed to create request. %v", err)
					return 0
				}

				resp, err = hcc.httpClient.Do(req)
				if err != nil {
					log.Printf("Failed to connect to host. %v", err)
					return 0
				}

				if resp.StatusCode != 200 {
					err := resp.Body.Close()
					if err != nil {
						log.Printf("Socket close failed handling status code != 200 (%s)", err)
					}
					if resp.StatusCode == 401 {
						log.Printf("Failed to connect to host, bad auth.")
						return -1
					} else if resp.StatusCode == 404 {
						if is2x {
							log.Printf("Failed to connect to host, bad bucket.")
							return -1
						}

						return doConfigRequest(true)
					}
					log.Printf("Failed to connect to host, unexpected status code: %v.", resp.StatusCode)
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

			log.Printf("Connected.")

			var autoDisconnected int32

			// Autodisconnect eventually
			go func() {
				select {
				case <-time.After(maxConnPeriod):
				case <-ctx.Done():
				}

				log.Printf("Automatically resetting our HTTP connection")

				atomic.StoreInt32(&autoDisconnected, 1)

				err := resp.Body.Close()
				if err != nil {
					log.Printf("Socket close failed during auto-dc (%s)", err)
				}
			}()

			dec := json.NewDecoder(resp.Body)
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

					log.Printf("Config block decode failure (%s)", err)

					if err != io.EOF {
						err = resp.Body.Close()
						if err != nil {
							log.Printf("Socket close failed after decode fail (%s)", err)
						}
					}

					break
				}

				log.Printf("Got Block: %v", string(configBlock.Bytes))

				bkCfg, err := parseConfig(configBlock.Bytes, hostname)
				if err != nil {
					log.Printf("Got error while parsing config: %v", err)

					err = resp.Body.Close()
					if err != nil {
						log.Printf("Socket close failed after parsing fail (%s)", err)
					}

					break
				}

				log.Printf("Got Config.")

				iterSawConfig = true
				log.Printf("HTTP Config Update")
				outputCh <- bkCfg
			}

			log.Printf("HTTP, Setting %s to iter %d", pickedSrv, iterNum)
		}
	}()

	return latestonlychannel.Wrap(outputCh), nil
}

func (hcc *httpConfigPoller) pickEndpoint(iterNum uint64) string {
	var pickedSrv string
	for _, srv := range hcc.endpoints.NonSSLEndpoints {
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
