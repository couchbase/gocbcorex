package gocbcore

import (
	"encoding/json"
	"errors"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *UnitTestSuite) TestAggregateErrorMarshals() {
	terr := &aggregateError{
		errors.New("some-error"),
		&TransactionOperationFailedError{
			shouldNotRetry:    true,
			shouldNotRollback: true,
			errorCause:        errors.New("some-cause"),
			shouldRaise:       TransactionErrorReasonTransactionExpired,
			errorClass:        TransactionErrorClassFailCasMismatch,
		},
	}

	bytes, err := json.Marshal(terr)
	suite.Require().Nil(err, "marshal failed")

	suite.Require().Equal([]byte(`["some-error",{"retry":false,"rollback":false,"raise":"expired","cause":"some-cause"}]`), bytes)
}

func (suite *UnitTestSuite) TestGocbcoreErrorMarshals() {
	terr := &TransactionOperationFailedError{
		shouldNotRetry:    true,
		shouldNotRollback: true,
		errorCause: KeyValueError{
			InnerError:         ErrCasMismatch,
			StatusCode:         memd.StatusAccessError,
			DocumentKey:        "key",
			BucketName:         "bucket",
			ScopeName:          "scope",
			CollectionName:     "collection",
			CollectionID:       19,
			ErrorName:          "",
			ErrorDescription:   "",
			Opaque:             4019,
			Context:            "",
			Ref:                "",
			RetryReasons:       nil,
			RetryAttempts:      1,
			LastDispatchedTo:   "127.0.0.1:11210",
			LastDispatchedFrom: "127.0.0.1:79654",
			LastConnectionID:   "",
		},
		shouldRaise: TransactionErrorReasonTransactionExpired,
		errorClass:  TransactionErrorClassFailCasMismatch,
	}

	bytes, err := json.Marshal(terr)
	suite.Require().Nil(err, "marshal failed")

	suite.Require().Equal([]byte(`{"retry":false,"rollback":false,"raise":"expired","cause":{"msg":"cas mismatch","status_code":36,"document_key":"key","bucket":"bucket","scope":"scope","collection":"collection","collection_id":19,"opaque":4019,"retry_attempts":1,"last_dispatched_to":"127.0.0.1:11210","last_dispatched_from":"127.0.0.1:79654"}}`), bytes)
}

func (suite *UnitTestSuite) TestForwardCompatError() {
	terr := &forwardCompatError{
		DocumentKey:    []byte("key"),
		BucketName:     "bucket",
		ScopeName:      "scope",
		CollectionName: "collection",
	}

	suite.Assert().ErrorIs(terr, ErrForwardCompatibilityFailure)
	suite.Assert().Equal(`forward compatibility error | bucket:bucket, scope:scope, collection:collection, key:key`, terr.Error())

	bytes, err := json.Marshal(terr)
	suite.Require().Nil(err, "marshal failed")

	suite.Assert().Equal(`{"bucket":"bucket","scope":"scope","collection":"collection","document_key":"key","msg":"forward compatibility error"}`, string(bytes))
}
