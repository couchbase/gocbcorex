package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"golang.org/x/exp/slices"
)

var clientRecordKey = []byte("_txn:client-record")

type jsonClientRecord struct {
	HeartbeatCas string `json:"heartbeat_ms,omitempty"`
	ExpiresMS    int    `json:"expires_ms,omitempty"`
	NumATRs      int    `json:"num_atrs,omitempty"`
}

type jsonClientOverride struct {
	Enabled      bool  `json:"enabled,omitempty"`
	ExpiresNanos int64 `json:"expires,omitempty"`
}

type jsonClientRecords struct {
	Clients  map[string]jsonClientRecord `json:"clients"`
	Override *jsonClientOverride         `json:"override,omitempty"`
}

type clientRecordDetails struct {
	OverrideActive    bool
	IndexOfThisClient int
	ActiveClientIds   []string
	ExpiredClientIDs  []string
	ThisClientAtrs    []string
}

func (c *LostTransactionCleaner) createClientRecord(ctx context.Context) error {
	err := c.clientRecordHooks.BeforeCreateRecord(ctx)
	if err != nil {
		return err
	}

	_, err = c.atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key: clientRecordKey,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDictAdd,
				Path:  []byte("records.clients"),
				Value: []byte("{}"),
				Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagMkDirP,
			},
			{
				Op:    memdx.MutateInOpTypeSetDoc,
				Value: []byte{0},
			},
		},
		Flags:          memdx.SubdocDocFlagAddDoc,
		CollectionName: c.atrCollectionName,
		ScopeName:      c.atrScopeName,
		OnBehalfOf:     c.atrOboUser,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *LostTransactionCleaner) fetchClientRecords(ctx context.Context) (*clientRecordDetails, error) {
	err := c.clientRecordHooks.BeforeGetRecord(ctx)
	if err != nil {
		return nil, err
	}

	result, err := c.atrAgent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key: clientRecordKey,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("records"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  memdx.SubdocXattrPathHLC,
				Flags: memdx.SubdocOpFlagXattrPath,
			},
		},
		CollectionName: c.atrCollectionName,
		ScopeName:      c.atrScopeName,
		OnBehalfOf:     c.atrOboUser,
	})
	if err != nil {
		return nil, err
	}

	recordOp := result.Ops[0]
	if recordOp.Err != nil {
		log.Printf("DEBUG: Failed to get records from client record for %s: %v", c.uuid, err)
		return nil, recordOp.Err
	}

	hlcOp := result.Ops[1]
	if hlcOp.Err != nil {
		log.Printf("DEBUG: Failed to get hlc from client record for %s: %v", c.uuid, err)
		return nil, hlcOp.Err
	}

	var records jsonClientRecords
	err = json.Unmarshal(recordOp.Value, &records)
	if err != nil {
		log.Printf("DEBUG: Failed to unmarshal records from client record for %s: %v", c.uuid, err)
		return nil, err
	}

	hlcNow, err := memdx.ParseHLCToTime(hlcOp.Value)
	if err != nil {
		log.Printf("DEBUG: Failed to parse hlc from client record for %s: %v", c.uuid, err)
		return nil, err
	}

	var hasThisClient bool
	var activeClientIds []string
	var expiredClientIds []string

	for clientUuid, client := range records.Clients {
		if clientUuid == c.uuid {
			// we don't check the heartbeat against ourselves
			activeClientIds = append(activeClientIds, clientUuid)
			hasThisClient = true
			continue
		}

		heartbeatCas, err := memdx.ParseMacroCasToCas([]byte(client.HeartbeatCas))
		if err != nil {
			return nil, wrapError(err, "failed to parse client record heartbeat cas")
		}

		heartbeatTime, err := memdx.ParseCasToTime(heartbeatCas)
		if err != nil {
			return nil, wrapError(err, "failed to parse client record heartbeat time")
		}

		heartbeatAge := hlcNow.Sub(heartbeatTime)
		if heartbeatAge >= time.Duration(client.ExpiresMS)*time.Millisecond {
			expiredClientIds = append(expiredClientIds, clientUuid)
		} else {
			activeClientIds = append(activeClientIds, clientUuid)
		}
	}

	// if our own client is missing, add it
	if !hasThisClient {
		activeClientIds = append(activeClientIds, c.uuid)
	}

	// sort the active client ids by their uuid
	sort.Strings(activeClientIds)

	thisClientIdx := -1
	for clientIdx, clientUuid := range activeClientIds {
		if clientUuid == c.uuid {
			thisClientIdx = clientIdx
		}
	}
	if thisClientIdx == -1 {
		// this should never be possible, since we add it ourselves, but just in case...
		return nil, errors.New("this client uuid was missing from the active ids list")
	}

	var overrideActive bool
	if records.Override != nil {
		if records.Override.Enabled {
			overrideExpiryTime := time.Unix(0, records.Override.ExpiresNanos)
			if overrideExpiryTime.Before(hlcNow) {
				overrideActive = true
			}
		}
	}

	numActiveClients := len(activeClientIds)
	numAtrs := c.numAtrs

	var atrsToHandle []string
	allAtrs := transactionAtrIDList[:numAtrs]
	for atrIdx := 0; atrIdx < len(allAtrs); atrIdx += numActiveClients {
		atrsToHandle = append(atrsToHandle, allAtrs[atrIdx])
	}

	return &clientRecordDetails{
		OverrideActive:    overrideActive,
		IndexOfThisClient: thisClientIdx,
		ActiveClientIds:   activeClientIds,
		ExpiredClientIDs:  expiredClientIds,
		ThisClientAtrs:    atrsToHandle,
	}, nil
}

func (c *LostTransactionCleaner) updateClientRecord(ctx context.Context, clientUuidsToRemove []string) ([]string, error) {
	log.Printf("SCHED: %s updating client record for %s.%s.%s", c.uuid, c.atrAgent.BucketName(), c.atrScopeName, c.atrCollectionName)

	err := c.clientRecordHooks.BeforeUpdateRecord(ctx)
	if err != nil {
		return nil, err
	}

	clientExpiryMs := (c.cleanupWindow + 20000*time.Millisecond).Milliseconds()

	var ops []memdx.MutateInOp
	ops = append(ops, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeDictSet,
		Path:  []byte(fmt.Sprintf("records.clients.%s.heartbeat_ms", c.uuid)),
		Value: memdx.SubdocMacroNewCas,
		Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagMkDirP | memdx.SubdocOpFlagExpandMacros,
	})
	ops = append(ops, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeDictSet,
		Path:  []byte(fmt.Sprintf("records.clients.%s.expires_ms", c.uuid)),
		Value: []byte(fmt.Sprintf("%d", clientExpiryMs)),
		Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagMkDirP,
	})
	ops = append(ops, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeDictSet,
		Path:  []byte(fmt.Sprintf("records.clients.%s.num_atrs", c.uuid)),
		Value: []byte(fmt.Sprintf("%d", c.numAtrs)),
		Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagMkDirP,
	})
	ops = append(ops, memdx.MutateInOp{
		Op:    memdx.MutateInOpTypeSetDoc,
		Value: []byte{0},
	})

	// fill up our remaining operations with expired client removals
	var removedClientUuids []string
	for _, clientUuid := range clientUuidsToRemove {
		if len(ops) >= 16 {
			// once we have 16 ops, we can't add anymore
			break
		}

		ops = append(ops, memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDelete,
			Path:  []byte(fmt.Sprintf("records.clients.%s", clientUuid)),
			Flags: memdx.SubdocOpFlagXattrPath,
		})

		removedClientUuids = append(removedClientUuids, clientUuid)
	}

	_, err = c.atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
		Key:            clientRecordKey,
		Ops:            ops,
		CollectionName: c.atrCollectionName,
		ScopeName:      c.atrScopeName,
		OnBehalfOf:     c.atrOboUser,
	})
	if err != nil {
		return nil, err
	}

	return removedClientUuids, nil
}

func (c *LostTransactionCleaner) processClient(ctx context.Context) ([]string, error) {
	clientDetails, err := c.fetchClientRecords(ctx)
	if err != nil {
		cerr := classifyError(err)
		if cerr.Class == TransactionErrorClassFailDocNotFound {
			// we ignore this error, and detect the missing client details below
		} else {
			return nil, wrapError(err, "failed to fetch client record (1st attempt)")
		}
	}

	if clientDetails == nil {
		// we are missing the client record, so we need to create it
		err = c.createClientRecord(ctx)
		if err != nil {
			cerr := classifyError(err)
			if cerr.Class == TransactionErrorClassFailDocAlreadyExists {
				// we ignore this error and simply refetch below
			} else {
				return nil, wrapError(err, "failed to create client record")
			}
		}

		newClientDetails, err := c.fetchClientRecords(ctx)
		if err != nil {
			// if we don't find the client record after just creating it, consider this
			// an error directly...
			return nil, wrapError(err, "failed to fetch client record (2nd attempt)")
		}

		clientDetails = newClientDetails
	}

	if clientDetails.OverrideActive {
		// if override is enabled, we don't do any updates here...
		return clientDetails.ThisClientAtrs, nil
	}

	// update the client record to refresh our heartbeat
	removedClientUuids, err := c.updateClientRecord(ctx, clientDetails.ExpiredClientIDs)
	if err != nil {
		return nil, wrapError(err, "failed to update client record")
	}

	// removed any expired clients from our client details
	newExpiredClientUuids := make([]string, 0, len(clientDetails.ExpiredClientIDs))
	for _, clientUuid := range clientDetails.ExpiredClientIDs {
		if !slices.Contains(removedClientUuids, clientUuid) {
			newExpiredClientUuids = append(newExpiredClientUuids, clientUuid)
		}
	}
	clientDetails.ExpiredClientIDs = newExpiredClientUuids

	return clientDetails.ThisClientAtrs, nil
}
