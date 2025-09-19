package memdx_test

import (
	"testing"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/stretchr/testify/require"
)

func TestOpsUtilsStats(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	opsUtils := memdx.OpsUtils{
		ExtFramesEnabled: false,
	}

	waitCh := make(chan error, 10)

	numStats := 0
	_, err := opsUtils.Stats(cli, &memdx.StatsRequest{
		GroupName: "",
	}, func(resp *memdx.StatsDataResponse) error {
		numStats++
		return nil
	}, func(resp *memdx.StatsActionResponse, err error) {
		waitCh <- err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = <-waitCh
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Greater(t, numStats, 0, "expected at least one stats entry")
}

func TestOpsUtilsStatsVbucketDetails(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	opsUtils := memdx.OpsUtils{
		ExtFramesEnabled: false,
	}

	vbIdValues := []*uint16{
		nil,
		ptr.To(uint16(defaultTestVbucketID)),
	}
	for _, vbucketId := range vbIdValues {
		waitCh := make(chan error, 10)

		parser := memdx.VbucketDetailsStatsParser{
			VbucketID: vbucketId,
		}
		_, err := opsUtils.Stats(cli, &memdx.StatsRequest{
			GroupName: parser.GroupName(),
		}, func(resp *memdx.StatsDataResponse) error {
			parser.HandleEntry(resp.Key, resp.Value)
			return nil
		}, func(resp *memdx.StatsActionResponse, err error) {
			waitCh <- err
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err = <-waitCh
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		require.Greater(t, len(parser.Vbuckets), 0, "expected at least one vbucket entry")
		for _, entry := range parser.Vbuckets {
			require.True(t, entry.UuidParsed, "expected entry uuid to be parsed")
			require.True(t, entry.HighSeqnoParsed, "expected entry high_seqno to be parsed")
			require.True(t, entry.MaxCasParsed, "expected entry max_cas to be parsed")
		}
	}
}

func TestOpsUtilsStatsFailover(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	opsUtils := memdx.OpsUtils{
		ExtFramesEnabled: false,
	}

	vbIdValues := []*uint16{
		nil,
		ptr.To(uint16(defaultTestVbucketID)),
	}
	for _, vbucketId := range vbIdValues {
		waitCh := make(chan error, 10)

		parser := memdx.FailoverStatsParser{
			VbucketID: vbucketId,
		}
		_, err := opsUtils.Stats(cli, &memdx.StatsRequest{
			GroupName: parser.GroupName(),
		}, func(resp *memdx.StatsDataResponse) error {
			parser.HandleEntry(resp.Key, resp.Value)
			return nil
		}, func(resp *memdx.StatsActionResponse, err error) {
			waitCh <- err
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err = <-waitCh
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		require.Greater(t, len(parser.Vbuckets), 0, "expected at least one vbucket entry")
		for _, entry := range parser.Vbuckets {
			require.True(t, entry.NumEntriesParsed, "expected entry num_entries to be parsed")
			require.True(t, entry.NumErroneousEntriesErasedParsed, "expected entry num_erroneous_entries_erased to be parsed")

			require.Greater(t, len(entry.FailoverLog), 0, "expected entry failover_log to have at least one entry")
			for _, logEntry := range entry.FailoverLog {
				require.True(t, logEntry.VbUuidParsed, "expected log entry vb_uuid to be parsed")
				require.True(t, logEntry.SeqnoParsed, "expected log entry seqno to be parsed")
			}
		}
	}
}

func TestOpsUtilsStatsVbucketSeqNo(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	opsUtils := memdx.OpsUtils{
		ExtFramesEnabled: false,
	}

	vbIdValues := []*uint16{
		nil,
		ptr.To(uint16(defaultTestVbucketID)),
	}
	for _, vbucketId := range vbIdValues {
		waitCh := make(chan error, 10)

		parser := memdx.VbucketSeqNoStatsParser{
			VbucketID: vbucketId,
		}
		_, err := opsUtils.Stats(cli, &memdx.StatsRequest{
			GroupName: parser.GroupName(),
		}, func(resp *memdx.StatsDataResponse) error {
			parser.HandleEntry(resp.Key, resp.Value)
			return nil
		}, func(resp *memdx.StatsActionResponse, err error) {
			waitCh <- err
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err = <-waitCh
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		require.Greater(t, len(parser.Vbuckets), 0, "expected at least one vbucket entry")
		for _, entry := range parser.Vbuckets {
			require.True(t, entry.UuidParsed, "expected entry uuid to be parsed")
			require.True(t, entry.HighSeqnoParsed, "expected entry high_seqno to be parsed")
		}
	}
}
