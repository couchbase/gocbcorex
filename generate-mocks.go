//go:generate moq -out mock_collectionresolver_test.go . CollectionResolver
//go:generate moq -out mock_vbucketrouter_test.go . VbucketRouter
//go:generate moq -out mock_kvclient_test.go . KvClient MemdxClient
//go:generate moq -out mock_kvclientpool_test.go . KvClientPool
//go:generate moq -out mock_kvclientbabysitter_test.go . KvClientBabysitter
//go:generate moq -out mock_kvendpointclientmanager_test.go . KvEndpointClientManager
//go:generate moq -out mock_retrymanager_test.go . RetryManager RetryController
//go:generate moq -out mock_bucketchecker_test.go . BucketChecker
//go:generate moq -out ./cbauthx/mock_authcheck_test.go ./cbauthx AuthCheck

package gocbcorex
