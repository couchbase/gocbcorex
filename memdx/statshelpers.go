package memdx

import (
	"fmt"
	"strconv"
)

// this file contains some helpers, mainly used by tests to synchronize
// the asynchronous calls that memdx uses

type StatsVbucketDetailsRequest struct {
	VbucketID uint16
}

func (r StatsVbucketDetailsRequest) OpName() string { return "Stats-VbucketDetails" }

type StatsVbucketDetailsResponse struct {
	HighSeqno *uint64
	MaxCas    *uint64
}

func StatsVbucketDetails(
	opsUtils OpsUtils,
	d Dispatcher,
	req *StatsVbucketDetailsRequest,
	cb func(*StatsVbucketDetailsResponse, error),
) (PendingOp, error) {
	groupName := fmt.Sprintf("vbucket-details %d", req.VbucketID)
	highSeqnoKey := fmt.Sprintf("vb_%d:high_seqno", req.VbucketID)
	maxCasKey := fmt.Sprintf("vb_%d:max_cas", req.VbucketID)

	resp := &StatsVbucketDetailsResponse{}
	return opsUtils.Stats(d, &StatsRequest{
		GroupName: groupName,
	}, func(dataResp *StatsDataResponse) {
		switch dataResp.Key {
		case highSeqnoKey:
			highSeqno, err := strconv.ParseUint(dataResp.Value, 10, 64)
			if err != nil {
				return
			}

			resp.HighSeqno = &highSeqno
		case maxCasKey:
			maxCas, err := strconv.ParseUint(dataResp.Value, 10, 64)
			if err != nil {
				return
			}

			resp.MaxCas = &maxCas
		}
	}, func(actionResp *StatsActionResponse, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		cb(resp, nil)
	})
}
