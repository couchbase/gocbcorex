package gocbcorex

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"golang.org/x/exp/slices"
)

type baseHttpComponent struct {
	serviceType ServiceType
	userAgent   string

	lock  sync.RWMutex
	state *baseHttpComponentState
}

type baseHttpComponentState struct {
	httpRoundTripper http.RoundTripper
	endpoints        map[string]string
	authenticator    Authenticator
}

type baseHttpTarget struct {
	EndpointId string
	Endpoint   string
	Username   string
	Password   string
}

func (c *baseHttpComponent) updateState(newState baseHttpComponentState) {
	c.lock.Lock()
	c.state = &newState
	c.lock.Unlock()
}

func (c *baseHttpComponent) GetAllTargets(endpointIdsToIgnore []string) (http.RoundTripper, []baseHttpTarget, error) {
	c.lock.RLock()
	state := *c.state
	c.lock.RUnlock()

	// remove all the endpoints we've already tried
	remainingEndpoints := make(map[string]string, len(state.endpoints))
	for epId, endpoint := range state.endpoints {
		if !slices.Contains(endpointIdsToIgnore, epId) {
			remainingEndpoints[epId] = endpoint
		}
	}

	targets := make([]baseHttpTarget, 0, len(remainingEndpoints))
	for epId, endpoint := range remainingEndpoints {
		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, []baseHttpTarget{}, err
		}

		username, password, err := state.authenticator.GetCredentials(c.serviceType, host)
		if err != nil {
			return nil, []baseHttpTarget{}, err
		}

		targets = append(targets, baseHttpTarget{
			EndpointId: epId,
			Endpoint:   endpoint,
			Username:   username,
			Password:   password,
		})
	}

	return state.httpRoundTripper, targets, nil
}

func (c *baseHttpComponent) SelectEndpoint(endpointIdsToIgnore []string) (http.RoundTripper, string, string, string, string, error) {
	c.lock.RLock()
	state := *c.state
	c.lock.RUnlock()

	// if there are no endpoints to query, we can't proceed
	if len(state.endpoints) == 0 {
		return nil, "", "", "", "", nil
	}

	// remove all the endpoints we've already tried
	remainingEndpoints := make(map[string]string, len(state.endpoints))
	endpointIds := make([]string, 0, len(state.endpoints))
	for epId, endpoint := range state.endpoints {
		if !slices.Contains(endpointIdsToIgnore, epId) {
			remainingEndpoints[epId] = endpoint
			endpointIds = append(endpointIds, epId)
		}
	}

	// if there are no more endpoints to try, we can't proceed
	if len(remainingEndpoints) == 0 {
		return nil, "", "", "", "", nil
	}

	// pick a random endpoint to attempt
	endpointIdx := rand.Intn(len(remainingEndpoints))
	endpointId := endpointIds[endpointIdx]
	endpoint := remainingEndpoints[endpointId]

	host, err := getHostFromUri(endpoint)
	if err != nil {
		return nil, "", "", "", "", err
	}

	username, password, err := state.authenticator.GetCredentials(c.serviceType, host)
	if err != nil {
		return nil, "", "", "", "", err
	}

	return state.httpRoundTripper, endpointId, endpoint, username, password, nil
}

type baseHttpTargets []baseHttpTarget

func (nt baseHttpTargets) ToMgmtx() []cbmgmtx.NodeTarget {
	targets := make([]cbmgmtx.NodeTarget, len(nt))
	for i, target := range nt {
		targets[i] = cbmgmtx.NodeTarget{
			Endpoint: target.Endpoint,
			Username: target.Username,
			Password: target.Password,
		}
	}

	return targets
}

func (nt baseHttpTargets) ToQueryx() []cbqueryx.NodeTarget {
	targets := make([]cbqueryx.NodeTarget, len(nt))
	for i, target := range nt {
		targets[i] = cbqueryx.NodeTarget{
			Endpoint: target.Endpoint,
			Username: target.Username,
			Password: target.Password,
		}
	}

	return targets
}

func (nt baseHttpTargets) ToSearchx() []cbsearchx.NodeTarget {
	targets := make([]cbsearchx.NodeTarget, len(nt))
	for i, target := range nt {
		targets[i] = cbsearchx.NodeTarget{
			Endpoint: target.Endpoint,
			Username: target.Username,
			Password: target.Password,
		}
	}

	return targets
}

func (c *baseHttpComponent) ensureResource(ctx context.Context, backoff BackoffCalculator,
	pollFn func(context.Context, http.RoundTripper, baseHttpTargets) (bool, error)) error {

	for attemptIdx := 0; ; attemptIdx++ {
		roundTripper, targets, err := c.GetAllTargets(nil)
		if err != nil {
			return err
		}

		success, err := pollFn(ctx, roundTripper, targets)
		if err != nil {
			return err
		}

		if success {
			break
		}

		select {
		case <-time.After(backoff(uint32(attemptIdx))):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
