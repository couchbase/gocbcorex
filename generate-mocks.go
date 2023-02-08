//go:generate moq -out mock_configmanager_test.go . ConfigManager
//go:generate moq -out mock_collectionresolver_test.go . CollectionResolver
//go:generate moq -out mock_vbucketrouter_test.go . VbucketRouter
//go:generate moq -out mock_httpclient_test.go . HTTPClient
//go:generate moq -out mock_basehttpclient_test.go . BaseHTTPClient
//go:generate moq -out mock_kvclient_test.go . KvClient MemdxDispatcherCloser
//go:generate moq -out mock_kvclientpool_test.go . KvClientPool
//go:generate moq -out mock_kvclientmanager_test.go . KvClientManager
//go:generate moq -out mock_retrymanager_test.go . RetryManager RetryController

package core
