package memdx

import (
	"fmt"
	"strconv"
)

// These parsers are used to parse stats entries from the Memcached server.  They
// keep track of which fields have been parsed and can be used to validate the
// stats entries returned by the server.

type VbucketDetailsStatsParser_Vbucket struct {
	Uuid      uint64
	HighSeqno uint64
	MaxCas    uint64

	UuidParsed      bool
	HighSeqnoParsed bool
	MaxCasParsed    bool
}

type VbucketDetailsStatsParser struct {
	VbucketID *uint16

	Vbuckets map[uint16]*VbucketDetailsStatsParser_Vbucket
}

func (p *VbucketDetailsStatsParser) GroupName() string {
	if p.VbucketID != nil {
		return fmt.Sprintf("vbucket-details %d", *p.VbucketID)
	} else {
		return "vbucket-details"
	}
}

func (p *VbucketDetailsStatsParser) getVbEntry(vbucketID uint16) *VbucketDetailsStatsParser_Vbucket {
	if p.Vbuckets == nil {
		p.Vbuckets = make(map[uint16]*VbucketDetailsStatsParser_Vbucket)
	}

	entry := p.Vbuckets[vbucketID]
	if entry == nil {
		entry = &VbucketDetailsStatsParser_Vbucket{}
		p.Vbuckets[vbucketID] = entry
	}

	return entry
}

func (p *VbucketDetailsStatsParser) HandleEntry(key string, value string) {
	var vbId uint16
	var subKey string
	_, err := fmt.Sscanf(key, "vb_%d:%s", &vbId, &subKey)
	if err != nil {
		return
	}

	entry := p.getVbEntry(vbId)

	switch subKey {
	case "uuid":
		uuid, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.Uuid = uuid
		entry.UuidParsed = true
		return
	case "high_seqno":
		highSeqno, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.HighSeqno = highSeqno
		entry.HighSeqnoParsed = true
		return
	case "max_cas":
		maxCas, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.MaxCas = maxCas
		entry.MaxCasParsed = true
		return
	}
}

type VbucketSeqNoStatsParser_Vbucket struct {
	Uuid      uint64
	HighSeqno uint64

	UuidParsed      bool
	HighSeqnoParsed bool
}

type VbucketSeqNoStatsParser struct {
	VbucketID *uint16

	Vbuckets map[uint16]*VbucketSeqNoStatsParser_Vbucket
}

func (p *VbucketSeqNoStatsParser) GroupName() string {
	if p.VbucketID != nil {
		return fmt.Sprintf("vbucket-seqno %d", *p.VbucketID)
	} else {
		return "vbucket-seqno"
	}
}

func (p *VbucketSeqNoStatsParser) getVbEntry(vbucketID uint16) *VbucketSeqNoStatsParser_Vbucket {
	if p.Vbuckets == nil {
		p.Vbuckets = make(map[uint16]*VbucketSeqNoStatsParser_Vbucket)
	}

	entry := p.Vbuckets[vbucketID]
	if entry == nil {
		entry = &VbucketSeqNoStatsParser_Vbucket{}
		p.Vbuckets[vbucketID] = entry
	}

	return entry
}

func (p *VbucketSeqNoStatsParser) HandleEntry(key string, value string) {
	var vbId uint16
	var subKey string
	_, err := fmt.Sscanf(key, "vb_%d:%s", &vbId, &subKey)
	if err != nil {
		return
	}

	entry := p.getVbEntry(vbId)

	switch subKey {
	case "uuid":
		uuid, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.Uuid = uuid
		entry.UuidParsed = true
		return
	case "high_seqno":
		highSeqno, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.HighSeqno = highSeqno
		entry.HighSeqnoParsed = true
		return
	}
}

type FailoverStatsParser_Vbucket_LogEntry struct {
	VbUuid uint64
	SeqNo  uint64

	VbUuidParsed bool
	SeqnoParsed  bool
}

type FailoverStatsParser_Vbucket struct {
	FailoverLog               []FailoverStatsParser_Vbucket_LogEntry
	NumErroneousEntriesErased uint

	NumEntriesParsed                bool
	NumErroneousEntriesErasedParsed bool
}

type FailoverStatsParser struct {
	VbucketID *uint16

	Vbuckets map[uint16]*FailoverStatsParser_Vbucket
}

func (p *FailoverStatsParser) GroupName() string {
	if p.VbucketID != nil {
		return fmt.Sprintf("failovers %d", *p.VbucketID)
	} else {
		return "failovers"
	}
}

func (p *FailoverStatsParser) getVbEntry(vbucketID uint16) *FailoverStatsParser_Vbucket {
	if p.Vbuckets == nil {
		p.Vbuckets = make(map[uint16]*FailoverStatsParser_Vbucket)
	}

	entry := p.Vbuckets[vbucketID]
	if entry == nil {
		entry = &FailoverStatsParser_Vbucket{}
		p.Vbuckets[vbucketID] = entry
	}

	return entry
}

func (p *FailoverStatsParser) HandleEntry(key string, value string) {
	var vbId uint16
	var subKey string
	_, err := fmt.Sscanf(key, "vb_%d:%s", &vbId, &subKey)
	if err != nil {
		return
	}

	entry := p.getVbEntry(vbId)

	switch subKey {
	case "num_entries":
		numEntries, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.FailoverLog = make([]FailoverStatsParser_Vbucket_LogEntry, numEntries)
		entry.NumEntriesParsed = true
		return
	case "num_erroneous_entries_erased":
		numErroneousEntriesErased, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}

		entry.NumErroneousEntriesErased = uint(numErroneousEntriesErased)
		entry.NumErroneousEntriesErasedParsed = true
		return
	}

	var entryIdx uint
	_, err = fmt.Sscanf(subKey, "%d:%s", &entryIdx, &subKey)
	if err == nil {
		if entryIdx >= uint(len(entry.FailoverLog)) {
			// If the entry index is out of bounds, we can't store it.
			return
		}

		logEntry := &entry.FailoverLog[entryIdx]

		switch subKey {
		case "id":
			vbUuid, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return
			}

			logEntry.VbUuid = vbUuid
			logEntry.VbUuidParsed = true
			return
		case "seq":
			seqno, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return
			}

			logEntry.SeqNo = seqno
			logEntry.SeqnoParsed = true
			return
		}
	}
}
