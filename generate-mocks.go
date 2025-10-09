//go:generate moq -fmt goimports -out mock_collectionresolver_test.go . CollectionResolver
//go:generate moq -fmt goimports -out mock_vbucketrouter_test.go . VbucketRouter
//go:generate moq -fmt goimports -out mock_kvclient_test.go . KvClient MemdxClient
//go:generate moq -fmt goimports -out mock_kvclientpool_test.go . KvClientPool
//go:generate moq -fmt goimports -out mock_kvclientmanager_test.go . KvClientManager
//go:generate moq -fmt goimports -out mock_retrymanager_test.go . RetryManager RetryController
//go:generate moq -fmt goimports -out mock_bucketchecker_test.go . BucketChecker
//go:generate moq -fmt goimports -out ./cbauthx/mock_authcheck_test.go ./cbauthx AuthCheck

package gocbcorex
