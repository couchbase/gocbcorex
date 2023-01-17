package core

import (
	"testing"

	"github.com/couchbase/stellar-nebula/core/memdx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvClientConnectAndBootstrapAndSendOp(t *testing.T) {
	t.SkipNow()

	client, err := newKvClient(kvClientOptions{
		Hostname:       "10.112.230.101:11210",
		TlsConfig:      nil,
		SelectedBucket: "default",
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
	require.NoError(t, err)

	res, err := client.Bootstrap()
	require.NoError(t, err)

	assert.NotNil(t, res.ClusterConfig)
	assert.NotNil(t, res.ErrorMap)

	respCh := make(chan *memdx.GetResponse, 1)
	errCh := make(chan error, 1)
	err = memdx.OpsCrud{
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
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case resp := <-respCh:
		assert.NotZero(t, resp.Cas)
		assert.NotEmpty(t, resp.Value)
	}
}
