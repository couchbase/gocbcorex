package gocbcorex

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
)

type baseHttpComponent struct {
	serviceType ServiceType
	userAgent   string

	lock  sync.RWMutex
	state *baseHttpComponentState
}

type baseHttpComponentState struct {
	httpRoundTripper http.RoundTripper
	endpoints        []string
	authenticator    Authenticator
}

type baseHttpTarget struct {
	Endpoint string
	Username string
	Password string
}

func (c *baseHttpComponent) updateState(newState baseHttpComponentState) {
	c.lock.Lock()
	c.state = &newState
	c.lock.Unlock()
}

func (c *baseHttpComponent) GetAllTargets(ignoredEndpoints []string) (http.RoundTripper, []baseHttpTarget, error) {
	c.lock.RLock()
	state := *c.state
	c.lock.RUnlock()

	// remove all the endpoints we've already tried
	remainingEndpoints := filterStringsOut(state.endpoints, ignoredEndpoints)

	targets := make([]baseHttpTarget, len(remainingEndpoints))
	for endpointIdx, endpoint := range remainingEndpoints {
		host, err := getHostFromUri(endpoint)
		if err != nil {
			return nil, []baseHttpTarget{}, err
		}

		username, password, err := state.authenticator.GetCredentials(c.serviceType, host)
		if err != nil {
			return nil, []baseHttpTarget{}, err
		}

		targets[endpointIdx] = baseHttpTarget{
			Endpoint: endpoint,
			Username: username,
			Password: password,
		}
	}

	return state.httpRoundTripper, targets, nil
}

func (c *baseHttpComponent) SelectEndpoint(ignoredEndpoints []string) (http.RoundTripper, string, string, string, error) {
	c.lock.RLock()
	state := *c.state
	c.lock.RUnlock()

	// if there are no endpoints to query, we can't proceed
	if len(state.endpoints) == 0 {
		return nil, "", "", "", nil
	}

	// remove all the endpoints we've already tried
	remainingEndpoints := filterStringsOut(state.endpoints, ignoredEndpoints)

	// if there are no more endpoints to try, we can't proceed
	if len(remainingEndpoints) == 0 {
		return nil, "", "", "", nil
	}

	// pick a random endpoint to attempt
	endpoint := remainingEndpoints[rand.Intn(len(remainingEndpoints))]

	host, err := getHostFromUri(endpoint)
	if err != nil {
		return nil, "", "", "", err
	}

	username, password, err := state.authenticator.GetCredentials(c.serviceType, host)
	if err != nil {
		return nil, "", "", "", err
	}

	return state.httpRoundTripper, endpoint, username, password, nil
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
