package zaputils

import (
	"fmt"

	"go.uber.org/zap"
)

func BucketName(key string, val string) zap.Field {
	return zap.String(key, val)
}

func ScopeName(key string, val string) zap.Field {
	return zap.String(key, val)
}

func CollectionName(key string, val string) zap.Field {
	return zap.String(key, val)
}

func DocID(key string, val []byte) zap.Field {
	return zap.String(key, string(val))
}

type LoggableFqDocID struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocID          []byte
}

func (e LoggableFqDocID) String() string {
	if e.DocID == nil {
		return fmt.Sprintf("%s/%s/%s", e.BucketName, e.ScopeName, e.CollectionName)
	}

	return fmt.Sprintf("%s/%s/%s/%s", e.BucketName, e.ScopeName, e.CollectionName, e.DocID)
}

func FQDocID(key string, bucket, scope, collection string, docID []byte) zap.Field {
	return zap.Stringer(key, LoggableFqDocID{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
		DocID:          docID,
	})
}

func FQCollectionName(key string, bucket, scope, collection string) zap.Field {
	// we just reuse the same logic as above
	return FQDocID(key, bucket, scope, collection, nil)
}
