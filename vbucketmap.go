package core

import "hash/crc32"

type vbucketMap struct {
	entries     [][]int
	numReplicas int
}

func newVbucketMap(entries [][]int, numReplicas int) *vbucketMap {
	vbMap := vbucketMap{
		entries:     entries,
		numReplicas: numReplicas,
	}
	return &vbMap
}

func (vbMap vbucketMap) IsValid() bool {
	return len(vbMap.entries) > 0 && len(vbMap.entries[0]) > 0
}

func (vbMap vbucketMap) NumVbuckets() int {
	return len(vbMap.entries)
}

func (vbMap vbucketMap) NumReplicas() int {
	return vbMap.numReplicas
}

func (vbMap vbucketMap) VbucketByKey(key []byte) uint16 {
	crc := crc32.ChecksumIEEE(key)
	crcMidBits := uint16(crc>>16) & ^uint16(0x8000)
	return crcMidBits % uint16(len(vbMap.entries))
}

func (vbMap vbucketMap) NodeByVbucket(vbID uint16, replicaID uint32) (int, error) {
	if vbID >= uint16(len(vbMap.entries)) {
		return 0, placeholderError{"errInvalidVBucket"}
	}

	if replicaID >= uint32(len(vbMap.entries[vbID])) {
		return 0, placeholderError{"errInvalidVBucket"}
	}

	return vbMap.entries[vbID][replicaID], nil
}

func (vbMap vbucketMap) VbucketsOnServer(index int) ([]uint16, error) {
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

func (vbMap vbucketMap) VbucketsByServer(replicaID int) ([][]uint16, error) {
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

func (vbMap vbucketMap) NodeByKey(key []byte, replicaID uint32) (int, error) {
	return vbMap.NodeByVbucket(vbMap.VbucketByKey(key), replicaID)
}
