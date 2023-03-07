package gocbcorex

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type MutationToken struct {
	VbID   uint16
	VbUuid uint64
	SeqNo  uint64
}

// MutationState holds and aggregates MutationToken's across multiple operations.
type MutationState struct {
	Tokens map[string][]MutationToken
}

// NewMutationState creates a new MutationState for tracking mutation state.
func NewMutationState(bucketName string, tokens ...MutationToken) *MutationState {
	mt := &MutationState{
		Tokens: make(map[string][]MutationToken),
	}
	mt.Add(bucketName, tokens...)
	return mt
}

// Add includes an operation's mutation information in this mutation state.
func (mt *MutationState) Add(bucketName string, tokens ...MutationToken) {
	if _, ok := mt.Tokens[bucketName]; !ok {
		mt.Tokens[bucketName] = []MutationToken{}
	}

	mt.Tokens[bucketName] = append(mt.Tokens[bucketName], tokens...)
}

// MarshalJSON marshal's this mutation state to JSON.
func (mt *MutationState) MarshalJSON() ([]byte, error) {
	var data mutationStateData
	for bucketName, tokens := range mt.Tokens {
		for _, token := range tokens {
			if data == nil {
				data = make(mutationStateData)
			}

			if (data)[bucketName] == nil {
				tokens := make(bucketTokens)
				(data)[bucketName] = &tokens
			}

			vbID := fmt.Sprintf("%d", token.VbID)
			stateToken := (*(data)[bucketName])[vbID]
			if stateToken == nil {
				stateToken = &bucketToken{}
				(*(data)[bucketName])[vbID] = stateToken
			}

			stateToken.SeqNo = token.SeqNo
			stateToken.VbUUID = fmt.Sprintf("%d", token.VbUuid)
		}
	}

	return json.Marshal(data)
}

// UnmarshalJSON unmarshal's a mutation state from JSON.
func (mt *MutationState) UnmarshalJSON(data []byte) error {
	var stateData mutationStateData
	err := json.Unmarshal(data, &stateData)
	if err != nil {
		return err
	}

	if mt.Tokens == nil {
		mt.Tokens = make(map[string][]MutationToken)
	}

	for bucketName, bTokens := range stateData {
		for vbIDStr, stateToken := range *bTokens {
			vbID, err := strconv.Atoi(vbIDStr)
			if err != nil {
				return err
			}
			vbUUID, err := strconv.Atoi(stateToken.VbUUID)
			if err != nil {
				return err
			}
			token := MutationToken{
				VbID:   uint16(vbID),
				VbUuid: uint64(vbUUID),
				SeqNo:  stateToken.SeqNo,
			}

			if _, ok := mt.Tokens[bucketName]; !ok {
				mt.Tokens[bucketName] = []MutationToken{}
			}

			mt.Tokens[bucketName] = append(mt.Tokens[bucketName], token)
		}
	}

	return nil
}

// toSearchMutationState is specific to search, search doesn't accept tokens in the same format as other services.
func (mt *MutationState) toSearchMutationState(indexName string) searchMutationState {
	data := make(searchMutationState)
	for _, tokens := range mt.Tokens {
		for _, token := range tokens {
			_, ok := data[indexName]
			if !ok {
				data[indexName] = make(map[string]uint64)
			}

			data[indexName][fmt.Sprintf("%d/%d", token.VbID, token.VbUuid)] = token.SeqNo
		}
	}

	return data
}

type bucketToken struct {
	SeqNo  uint64 `json:"seqno"`
	VbUUID string `json:"vbuuid"`
}

func (mt bucketToken) MarshalJSON() ([]byte, error) {
	info := []interface{}{mt.SeqNo, mt.VbUUID}
	return json.Marshal(info)
}

func (mt *bucketToken) UnmarshalJSON(data []byte) error {
	info := []interface{}{&mt.SeqNo, &mt.VbUUID}
	return json.Unmarshal(data, &info)
}

type bucketTokens map[string]*bucketToken
type mutationStateData map[string]*bucketTokens
type searchMutationState map[string]map[string]uint64
