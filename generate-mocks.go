//go:generate moq -out mock_collectionresolver_test.go . CollectionResolver
//go:generate moq -out mock_kvclient_test.go . KvClient
//go:generate moq -out mock_retrymanager_test.go . RetryManager RetryController

package core
