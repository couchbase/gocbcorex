package memdx

// HelloFeature represents a feature code included in a memcached
// HELLO operation.
type HelloFeature uint16

const (
	// FeatureDatatype indicates support for Datatype fields.
	HelloFeatureDatatype = HelloFeature(0x01)

	// FeatureTLS indicates support for TLS
	HelloFeatureTLS = HelloFeature(0x02)

	// FeatureTCPNoDelay indicates support for TCP no-delay.
	HelloFeatureTCPNoDelay = HelloFeature(0x03)

	// FeatureSeqNo indicates support for mutation tokens.
	HelloFeatureSeqNo = HelloFeature(0x04)

	// FeatureTCPDelay indicates support for TCP delay.
	HelloFeatureTCPDelay = HelloFeature(0x05)

	// FeatureXattr indicates support for document xattrs.
	HelloFeatureXattr = HelloFeature(0x06)

	// FeatureXerror indicates support for extended errors.
	HelloFeatureXerror = HelloFeature(0x07)

	// FeatureSelectBucket indicates support for the SelectBucket operation.
	HelloFeatureSelectBucket = HelloFeature(0x08)

	// Feature 0x09 is reserved and cannot be used.

	// FeatureSnappy indicates support for snappy compressed documents.
	HelloFeatureSnappy = HelloFeature(0x0a)

	// FeatureJSON indicates support for JSON datatype data.
	HelloFeatureJSON = HelloFeature(0x0b)

	// FeatureDuplex indicates support for duplex communications.
	HelloFeatureDuplex = HelloFeature(0x0c)

	// FeatureClusterMapNotif indicates support for cluster-map update notifications.
	HelloFeatureClusterMapNotif = HelloFeature(0x0d)

	// FeatureUnorderedExec indicates support for unordered execution of operations.
	HelloFeatureUnorderedExec = HelloFeature(0x0e)

	// FeatureDurations indicates support for server durations.
	HelloFeatureDurations = HelloFeature(0xf)

	// FeatureAltRequests indicates support for requests with flexible frame extras.
	HelloFeatureAltRequests = HelloFeature(0x10)

	// FeatureSyncReplication indicates support for requests synchronous durability requirements.
	HelloFeatureSyncReplication = HelloFeature(0x11)

	// FeatureCollections indicates support for collections.
	HelloFeatureCollections = HelloFeature(0x12)

	// FeatureOpenTracing indicates support for OpenTracing.
	HelloFeatureOpenTracing = HelloFeature(0x13)

	// FeaturePreserveExpiry indicates support for preserve TTL.
	HelloFeaturePreserveExpiry = HelloFeature(0x14)

	// FeaturePITR indicates support for PITR snapshots.
	HelloFeaturePITR = HelloFeature(0x16)

	// FeatureCreateAsDeleted indicates support for the create as deleted feature.
	HelloFeatureCreateAsDeleted = HelloFeature(0x17)

	// FeatureReplaceBodyWithXattr indicates support for the replace body with xattr feature.
	HelloFeatureReplaceBodyWithXattr = HelloFeature(0x19)
)
