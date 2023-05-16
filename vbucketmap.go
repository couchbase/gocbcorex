package gocbcorex

import (
	"errors"
	"hash/crc32"
)

type VbucketMap struct {
	entries     [][]int
	numReplicas int
}

func NewVbucketMap(entries [][]int, numReplicas int) (*VbucketMap, error) {
	if len(entries) == 0 {
		return nil, errors.New("vbucket map must have at least a single entry")
	}

	vbMap := VbucketMap{
		entries:     entries,
		numReplicas: numReplicas,
	}
	return &vbMap, nil
}

func (vbMap VbucketMap) IsValid() bool {
	return len(vbMap.entries) > 0 && len(vbMap.entries[0]) > 0
}

func (vbMap VbucketMap) NumVbuckets() int {
	return len(vbMap.entries)
}

func (vbMap VbucketMap) NumReplicas() int {
	return vbMap.numReplicas
}

func (vbMap VbucketMap) VbucketByKey(key []byte) uint16 {
	crc := crc32.ChecksumIEEE(key)
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	return crcMidBits % uint16(len(vbMap.entries))
}

func (vbMap VbucketMap) NodeByVbucket(vbID uint16, replicaID uint32) (int, error) {
	numVbs := uint16(len(vbMap.entries))
	if vbID >= numVbs {
		return 0, invalidVbucketError{
			RequestedVbId: vbID,
			NumVbuckets:   numVbs,
		}
	}

	numServers := uint32(vbMap.numReplicas) + 1
	if replicaID >= numServers {
		return 0, invalidReplicaError{
			RequestedReplica: replicaID,
			NumServers:       numServers,
		}
	}

	if replicaID >= uint32(len(vbMap.entries[vbID])) {
		return -1, nil
	}

	return vbMap.entries[vbID][replicaID], nil
}

func (vbMap VbucketMap) VbucketsOnServer(index int) ([]uint16, error) {
	vbList, err := vbMap.VbucketsByServer(0)
	if err != nil {
		return nil, err
	}

	if len(vbList) <= index {
		// Invalid server index
		return nil, placeholderError{"errInvalidReplica"}
	}

	return vbList[index], nil
}

func (vbMap VbucketMap) VbucketsByServer(replicaID int) ([][]uint16, error) {
	var vbList [][]uint16

	// We do not currently support listing for all replicas at once
	if replicaID < 0 {
		return nil, placeholderError{"errInvalidReplica"}
	}

	for vbID, entry := range vbMap.entries {
		if len(entry) <= replicaID {
			continue
		}

		serverID := entry[replicaID]

		for len(vbList) <= serverID {
			vbList = append(vbList, nil)
		}

		if vbList != nil {
			vbList[serverID] = append(vbList[serverID], uint16(vbID))
		}
	}

	return vbList, nil
}

func (vbMap VbucketMap) NodeByKey(key []byte, replicaID uint32) (int, error) {
	return vbMap.NodeByVbucket(vbMap.VbucketByKey(key), replicaID)
}
