package core

import (
	"testing"

	"github.com/couchbase/stellar-nebula/core/memdx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnManagerUpdateEndpointsAndExecute(t *testing.T) {
	t.SkipNow()

	connMgr := newConnectionManager(connManagerOptions{
		ConnectionsPerNode: 1,
		TlsConfig:          nil,
		SelectedBucket:     "default",
		Features: []memdx.HelloFeature{
			memdx.HelloFeatureTLS, memdx.HelloFeatureXattr, memdx.HelloFeatureSelectBucket, memdx.HelloFeatureXerror,
			memdx.HelloFeatureJSON, memdx.HelloFeatureSeqNo, memdx.HelloFeatureSnappy, memdx.HelloFeatureDurations,
			memdx.HelloFeatureCollections, memdx.HelloFeatureUnorderedExec, memdx.HelloFeatureAltRequests,
			memdx.HelloFeatureCreateAsDeleted, memdx.HelloFeatureReplaceBodyWithXattr, memdx.HelloFeaturePITR,
			memdx.HelloFeatureSyncReplication,
		},
		Username: "Administrator",
		Password: "password",
	})

	err := connMgr.UpdateEndpoints([]string{"10.112.230.101:11210"})
	require.NoError(t, err)

	respCh := make(chan *memdx.GetResponse, 1)
	errCh := make(chan error, 1)
	err = connMgr.Execute("10.112.230.101:11210", func(client KvClient, err error) error {
		return memdx.OpsCrud{
			CollectionsEnabled: true,
		}.Get(client, &memdx.GetRequest{
			CollectionID: 0,
			Key:          []byte("test"),
			VbucketID:    127,
		}, func(response *memdx.GetResponse, err error) {
			if err != nil {
				errCh <- err
				return
			}

			respCh <- response
		})
	})
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case resp := <-respCh:
		assert.NotZero(t, resp.Cas)
		assert.NotEmpty(t, resp.Value)
	}
}
