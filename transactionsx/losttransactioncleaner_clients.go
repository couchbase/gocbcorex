package transactionsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/zaputils"

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

// ClientRecordDetails describes the state of the client record in a collection
// at the moment it was read, from the point of view of one particular client.
type ClientRecordDetails struct {
	// ClientUUID is the uuid of the client this view was computed for.
	ClientUUID string

	// IndexOfThisClient is this client's position in ActiveClientIDs, which
	// determines the subset of ATRs it is responsible for.
	IndexOfThisClient int

	// ActiveClientIDs are the clients whose heartbeats are current, sorted by
	// uuid.  Always includes this client.
	ActiveClientIDs []string

	// ExpiredClientIDs are the clients whose heartbeats have aged out.
	ExpiredClientIDs []string

	// ThisClientAtrs are the ATRs this client should process.
	ThisClientAtrs []string

	// OverrideEnabled reports whether an override is recorded at all;
	// OverrideActive whether it is both recorded and still in force.  While an
	// override is active, clients do not update the record.
	OverrideEnabled      bool
	OverrideActive       bool
	OverrideExpiresNanos int64

	// CasNowNanos is the collection's HLC at the time of reading, which is the
	// clock all the expiry decisions above were made against.
	CasNowNanos int64
}

// NumActiveClients returns the number of clients with current heartbeats.
func (d *ClientRecordDetails) NumActiveClients() int {
	return len(d.ActiveClientIDs)
}

// NumExpiredClients returns the number of clients whose heartbeats have aged out.
func (d *ClientRecordDetails) NumExpiredClients() int {
	return len(d.ExpiredClientIDs)
}

// NumExistingClients returns the total number of clients in the record.
func (d *ClientRecordDetails) NumExistingClients() int {
	return len(d.ActiveClientIDs) + len(d.ExpiredClientIDs)
}

func (c *LostTransactionCleaner) createClientRecord(ctx context.Context) error {
	return invokeNoResHook(ctx, c.clientRecordHooks.CreateRecord, func() error {
		_, err := c.atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
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
		return err
	})
}

func (c *LostTransactionCleaner) fetchClientRecords(ctx context.Context) (*ClientRecordDetails, error) {
	return invokeHook(ctx, c.clientRecordHooks.GetRecord, func() (*ClientRecordDetails, error) {
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
			return nil, recordOp.Err
		}

		hlcOp := result.Ops[1]
		if hlcOp.Err != nil {
			return nil, hlcOp.Err
		}

		var records jsonClientRecords
		err = json.Unmarshal(recordOp.Value, &records)
		if err != nil {
			return nil, err
		}

		hlcNow, err := memdx.ParseHLCToTime(hlcOp.Value)
		if err != nil {
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

		var overrideEnabled bool
		var overrideActive bool
		var overrideExpiresNanos int64
		if records.Override != nil {
			overrideEnabled = records.Override.Enabled
			overrideExpiresNanos = records.Override.ExpiresNanos

			// An override holds until its expiry passes, so it is active while
			// the expiry is still in the future.
			overrideExpiryTime := time.Unix(0, overrideExpiresNanos)
			if overrideEnabled && hlcNow.Before(overrideExpiryTime) {
				overrideActive = true
			}
		}

		numActiveClients := len(activeClientIds)
		numAtrs := c.numAtrs

		var atrsToHandle []string
		allAtrs := AtrIDList[:numAtrs]
		for atrIdx := 0; atrIdx < len(allAtrs); atrIdx += numActiveClients {
			atrsToHandle = append(atrsToHandle, allAtrs[atrIdx])
		}

		return &ClientRecordDetails{
			ClientUUID:        c.uuid,
			IndexOfThisClient: thisClientIdx,
			ActiveClientIDs:   activeClientIds,
			ExpiredClientIDs:  expiredClientIds,
			ThisClientAtrs:    atrsToHandle,

			OverrideEnabled:      overrideEnabled,
			OverrideActive:       overrideActive,
			OverrideExpiresNanos: overrideExpiresNanos,

			CasNowNanos: hlcNow.UnixNano(),
		}, nil
	})
}

func (c *LostTransactionCleaner) updateClientRecord(ctx context.Context, clientUuidsToRemove []string) ([]string, error) {
	c.logger.Debug("updating client record",
		zap.String("uuid", c.uuid),
		zaputils.FQCollectionName("collection", c.atrAgent.BucketName(), c.atrScopeName, c.atrCollectionName))

	return invokeHook(ctx, c.clientRecordHooks.UpdateRecord, func() ([]string, error) {
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

		_, err := c.atrAgent.MutateIn(ctx, &gocbcorex.MutateInOptions{
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
	})
}

// ProcessClient refreshes this client's entry in the collection's client
// record and returns the resulting view of the record.
//
// This is the step LostTransactionCleaner performs at the start of every
// cleanup cycle to work out which ATRs it owns.  It is exported so that the
// record can also be driven and inspected directly.
func (c *LostTransactionCleaner) ProcessClient(ctx context.Context) (*ClientRecordDetails, error) {
	return c.processClient(ctx)
}

func (c *LostTransactionCleaner) processClient(ctx context.Context) (*ClientRecordDetails, error) {
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
		return clientDetails, nil
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

	return clientDetails, nil
}
