package gocbcorex

import "context"

type CollectionCheckerManifest struct {
	ManifestFetcher ManifestFetcher
}

var _ CollectionChecker = (*CollectionCheckerManifest)(nil)

func (c *CollectionCheckerManifest) HasScope(ctx context.Context, bucketName, scopeName string) (bool, error) {
	manifest, err := c.ManifestFetcher.GetManifest(ctx, bucketName)
	if err != nil {
		return false, contextualError{
			Cause:   err,
			Message: "failed to fetch manifest",
		}
	}

	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			return true, nil
		}
	}

	return false, nil
}

func (c *CollectionCheckerManifest) HasCollection(ctx context.Context, bucketName, scopeName, collectionName string) (bool, error) {
	manifest, err := c.ManifestFetcher.GetManifest(ctx, bucketName)
	if err != nil {
		return false, contextualError{
			Cause:   err,
			Message: "failed to fetch manifest",
		}
	}

	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			for _, collection := range scope.Collections {
				if collection.Name == collectionName {
					return true, nil
				}
			}
		}
	}

	return false, nil
}
