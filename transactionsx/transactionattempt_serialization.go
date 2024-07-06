package transactionsx

import (
	"context"
	"fmt"
)

type SerializedMutation struct {
	Bucket     string `json:"bkt"`
	Scope      string `json:"scp"`
	Collection string `json:"coll"`
	ID         string `json:"id"`
	Cas        string `json:"cas"`
	Type       string `json:"type"`
}

type SerializedAttempt struct {
	ID struct {
		Transaction string `json:"txn"`
		Attempt     string `json:"atmpt"`
	} `json:"id"`
	ATR struct {
		Bucket     string `json:"bkt"`
		Scope      string `json:"scp"`
		Collection string `json:"coll"`
		ID         string `json:"id"`
	} `json:"atr"`
	Config struct {
		KeyValueTimeoutMs int    `json:"kvTimeoutMs"`
		DurabilityLevel   string `json:"durabilityLevel"`
		NumAtrs           int    `json:"numAtrs"`
	} `json:"config"`
	State struct {
		TimeLeftMs int `json:"timeLeftMs"`
	} `json:"state"`
	Mutations []SerializedMutation `json:"mutations"`
}

func (t *TransactionAttempt) Serialize(ctx context.Context) (*SerializedAttempt, error) {
	res := &SerializedAttempt{}

	t.lock.Lock()
	defer t.lock.Unlock()

	errSt := t.waitForOpsLocked(ctx)
	if errSt != nil {
		t.lock.Unlock()
		return nil, errSt.Err()
	}

	if errSt := t.checkCanCommitLocked(); errSt != nil {
		return nil, errSt.Err()
	}

	res.ID.Transaction = t.transactionID
	res.ID.Attempt = t.id

	if t.atrAgent != nil {
		res.ATR.Bucket = t.atrAgent.BucketName()
		res.ATR.Scope = t.atrScopeName
		res.ATR.Collection = t.atrCollectionName
		res.ATR.ID = string(t.atrKey)
	} else if t.atrLocation.Agent != nil {
		res.ATR.Bucket = t.atrLocation.Agent.BucketName()
		res.ATR.Scope = t.atrLocation.ScopeName
		res.ATR.Collection = t.atrLocation.CollectionName
		res.ATR.ID = ""
	}

	res.Config.DurabilityLevel = durabilityLevelToString(t.durabilityLevel)
	res.Config.NumAtrs = 1024

	// we set a static timeout here to ensure that transactions work with older clients
	// which still leverage this value, this isn't relevant for gocbcorex.
	res.Config.KeyValueTimeoutMs = 2500

	res.State.TimeLeftMs = int(t.TimeRemaining().Milliseconds())

	for _, mutation := range t.stagedMutations {
		var mutationData SerializedMutation

		mutationData.Bucket = mutation.Agent.BucketName()
		mutationData.Scope = mutation.ScopeName
		mutationData.Collection = mutation.CollectionName
		mutationData.ID = string(mutation.Key)
		mutationData.Cas = fmt.Sprintf("%d", mutation.Cas)
		mutationData.Type = stagedMutationTypeToString(mutation.OpType)

		res.Mutations = append(res.Mutations, mutationData)
	}
	if len(res.Mutations) == 0 {
		res.Mutations = []SerializedMutation{}
	}

	return res, nil
}
