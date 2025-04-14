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
	if len(vbMap.entries) == 0 {
		// prevent divide-by-zero panic's
		return 0
	}

	crc := crc32.ChecksumIEEE(key)
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	return crcMidBits % uint16(len(vbMap.entries))
}

func (vbMap VbucketMap) NodeByVbucket(vbID uint16, vbServerIdx uint32) (int, error) {
	numVbs := uint16(len(vbMap.entries))
	if vbID >= numVbs {
		return 0, invalidVbucketError{
			RequestedVbId: vbID,
			NumVbuckets:   numVbs,
		}
	}

	numServers := uint32(vbMap.numReplicas) + 1
	if vbServerIdx >= numServers {
		return 0, invalidReplicaError{
			RequestedReplica: vbServerIdx,
			NumServers:       numServers,
		}
	}

	if vbServerIdx >= uint32(len(vbMap.entries[vbID])) {
		return -1, nil
	}

	return vbMap.entries[vbID][vbServerIdx], nil
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

func (vbMap VbucketMap) VbucketsByServer(vbServerIdx int) ([][]uint16, error) {
	var vbList [][]uint16

	// We do not currently support listing for all replicas at once
	if vbServerIdx < 0 {
		return nil, placeholderError{"errInvalidReplica"}
	}

	for vbID, entry := range vbMap.entries {
		if len(entry) <= vbServerIdx {
			continue
		}

		serverID := entry[vbServerIdx]

		for len(vbList) <= serverID {
			vbList = append(vbList, nil)
		}

		if vbList != nil {
			vbList[serverID] = append(vbList[serverID], uint16(vbID))
		}
	}

	return vbList, nil
}

func (vbMap VbucketMap) NodeByKey(key []byte, vbServerIdx uint32) (int, error) {
	return vbMap.NodeByVbucket(vbMap.VbucketByKey(key), vbServerIdx)
}
