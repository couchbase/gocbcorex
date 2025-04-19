package memdx

type ClustermapChangeEvent struct {
	BucketName []byte
	RevEpoch   int64
	Rev        int64
	Config     []byte
}
