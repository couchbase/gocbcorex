package memdx

import (
	"reflect"
	"testing"

	"github.com/couchbase/stellar-nebula/core/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsCrudGets(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op            func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name          string
		CheckOverride func(t *testing.T, res interface{})
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Get(cli, &GetRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetRandom",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetRandom(cli, &GetRandomRequest{}, func(resp *GetRandomResponse, err error) {
					cb(resp, err)
				})
			},
			CheckOverride: func(t *testing.T, res interface{}) {
				randRes, ok := res.(*GetRandomResponse)
				if !ok {
					t.Fatalf("Result of GetRandom was not *GetRandomResponse: %v", res)
				}

				assert.NotZero(t, randRes.Cas)
				assert.NotZero(t, randRes.Key)
				assert.NotZero(t, randRes.Value)
			},
		},
		// { TODO: This locks the doc, probably need to an unlock here too.
		// 	Name: "GetAndLock",
		// 	Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.GetAndLock(cli, &GetAndLockRequest{
		// 			Key:       key,
		// 			VbucketID: 1,
		// 			LockTime:  0,
		// 		}, func(resp *GetAndLockResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "GetAndTouch",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &GetAndTouchRequest{
					Key:       key,
					VbucketID: 1,
					Expiry:    60,
				}, func(resp *GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	_, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Set, cli, &SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			_, err = test.Op(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			res := <-waitres

			if test.CheckOverride != nil {
				test.CheckOverride(tt, res)
				return
			}

			elem := reflect.ValueOf(res).Elem()
			assert.Equal(tt, value, elem.FieldByName("Value").Bytes())
			// assert.Equal(tt, datatype, elem.FieldByName("Datatype").Interface().(uint8))	TODO: server is responding with 0
			assert.NotZero(tt, elem.FieldByName("Cas"))
		})
	}
}

func TestOpsCrudGetKeyNotFound(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	key := []byte(uuid.NewString())
	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Get(cli, &GetRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndLock",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndLock(cli, &GetAndLockRequest{
					Key: key,
				}, func(resp *GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &GetAndTouchRequest{
					Key: key,
				}, func(resp *GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, ErrDocNotFound)
		})
	}
}

func TestOpsCrudGetCollectionNotKnown(t *testing.T) {
	if !testutils.TestOpts.LongTest {
		t.SkipNow()
	}

	key := []byte(uuid.NewString())
	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Get(cli, &GetRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
				}, func(resp *GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetRandom",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetRandom(cli, &GetRandomRequest{
					CollectionID: 2222,
				}, func(resp *GetRandomResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndLock",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndLock(cli, &GetAndLockRequest{
					CollectionID: 2222,
					Key:          key,
				}, func(resp *GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &GetAndTouchRequest{
					CollectionID: 2222,
					Key:          key,
				}, func(resp *GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Set",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        key,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, ErrUnknownCollectionID)
		})
	}
}
