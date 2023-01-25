package core

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
	"github.com/couchbase/stellar-nebula/core/memdx"
	"log"
)

type ConfigManager interface {
	ApplyConfig(sourceHostname string, json *cbconfig.TerseConfigJson) (*routeConfig, bool)
	DispatchByKey(ctx context.Context, key []byte) (string, uint16, error)
	DispatchToVbucket(ctx context.Context, vbID uint16) (string, error)
	Reconfigure(tlsConfig *tls.Config)
	Close() error
}

func OrchestrateConfig[RespT any](
	ctx context.Context,
	cm ConfigManager,
	key []byte,
	fn func(endpoint string, vbID uint16) (RespT, error),
) (RespT, error) {
	for {
		endpoint, vbID, err := cm.DispatchByKey(ctx, key)
		if err != nil {
			var emptyResp RespT
			return emptyResp, err
		}

		// Implement me properly
		res, err := fn(endpoint, vbID)
		if err != nil {
			if errors.Is(err, memdx.ErrNotMyVbucket) {
				nmvberr := err.(memdx.NotMyVbucketError)
				cfg, parseErr := parseConfig(nmvberr.ConfigValue, endpoint)
				if parseErr == nil {
					cm.ApplyConfig(cfg.SourceHostname, cfg.Config)
					continue
				}
				log.Printf("Failed to parse not my vbucket response: %s", parseErr)
			}

			return res, err
		}

		return res, nil
	}
}
