package memdx_test

import (
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsCrudGets(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op            func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name          string
		CheckOverride func(t *testing.T, res interface{})
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Get(cli, &memdx.GetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &memdx.GetAndTouchRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Expiry:    60,
				}, func(resp *memdx.GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetMeta(cli, &memdx.GetMetaRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
			CheckOverride: func(t *testing.T, res interface{}) {
				randRes, ok := res.(*memdx.GetMetaResponse)
				if !ok {
					t.Fatalf("Result of GetRandom was not *GetRandomResponse: %v", res)
				}

				assert.Empty(t, randRes.Value)
				assert.NotZero(t, randRes.Cas)
				assert.Zero(t, randRes.Flags)
				assert.NotZero(t, randRes.Expiry)
				assert.NotZero(t, randRes.SeqNo)
				assert.False(t, randRes.Deleted)
				assert.Equal(t, datatype, randRes.Datatype)
			},
		},
		{
			Name: "LookupIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.LookupIn(cli, &memdx.LookupInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.LookupInOp{
						{
							Op:   memdx.LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *memdx.LookupInResponse, err error) {
					cb(resp, err)
				})
			},
			CheckOverride: func(t *testing.T, res interface{}) {
				randRes, ok := res.(*memdx.LookupInResponse)
				if !ok {
					t.Fatalf("Result of LookupIn was not *LookupInResponse: %v", res)
				}

				res0 := randRes.Ops[0]

				if assert.NoError(t, res0.Err) {
					assert.Equal(t, []byte(`"value"`), res0.Value)
				}

				assert.NotZero(t, randRes.Cas)
				assert.False(t, randRes.DocIsDeleted)
			},
		},
	}

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			_, err = test.Op(memdx.OpsCrud{
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

func TestOpsCrudKeyNotFound(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op   func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Get(cli, &memdx.GetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndLock",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndLock(cli, &memdx.GetAndLockRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &memdx.GetAndTouchRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Unlock",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Unlock(cli, &memdx.UnlockRequest{
					Key:       key,
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.UnlockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Touch(cli, &memdx.TouchRequest{
					Key:       key,
					Expiry:    60,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetMeta(cli, &memdx.GetMetaRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// Server is always responding with exists
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			Options:   0x08, // SKIP_CONFLICT_RESOLUTION_FLAG
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "LookupIn",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.LookupIn(cli, &memdx.LookupInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.LookupInOp{
						{
							Op:   memdx.LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *memdx.LookupInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			key := []byte(uuid.NewString()[:6])

			_, err := test.Op(key, memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, memdx.ErrDocNotFound)
		})
	}
}

func TestOpsCrudCollectionNotKnown(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Get(cli, &memdx.GetRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// The server is hanging on this request.
		// {
		// 	Name: "GetRandom",
		// 	Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.GetRandom(cli, &GetRandomRequest{
		// 			CollectionID: 2222,
		// 		}, func(resp *GetRandomResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "GetAndLock",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndLock(cli, &memdx.GetAndLockRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &memdx.GetAndTouchRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Unlock",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Unlock(cli, &memdx.UnlockRequest{
					CollectionID: 2222,
					Key:          key,
					Cas:          1,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.UnlockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Touch(cli, &memdx.TouchRequest{
					CollectionID: 2222,
					Key:          key,
					Expiry:       60,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					CollectionID: 2222,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					CollectionID: 2222,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetMeta(cli, &memdx.GetMetaRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "SetMeta",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.SetMeta(cli, &memdx.SetMetaRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					Cas:          1, // For some reason Cas is required here.
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.SetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			CollectionID: 2222,
		// 			Key:          key,
		// 			// 			Options:   0x08, // SKIP_CONFLICT_RESOLUTION_FLAG
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "LookupIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.LookupIn(cli, &memdx.LookupInRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
					Ops: []memdx.LookupInOp{
						{
							Op:   memdx.LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *memdx.LookupInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, memdx.ErrUnknownCollectionID)
		})
	}
}

func TestOpsCrudDocLocked(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("1")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op       func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name     string
		IsReadOp bool
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Get(cli, &memdx.GetRequest{
					CollectionID: 0,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.GetResponse, err error) {
					cb(resp, err)
				})
			},
			IsReadOp: true,
		},
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					CollectionID: 0,
					Key:          key,
					Value:        key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Touch(cli, &memdx.TouchRequest{
					CollectionID: 0,
					Key:          key,
					Expiry:       60,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// normally this would screw things up, but it should fail due to the doc being locked
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					CollectionID: 0,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					CollectionID: 0,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					CollectionID: 0,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					CollectionID: 0,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					CollectionID: 0,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					Delta:        1,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					CollectionID: 0,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					Delta:        1,
					VbucketID:    defaultTestVbucketID,
				}, func(resp *memdx.DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "LookupIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.LookupIn(cli, &memdx.LookupInRequest{
					CollectionID: 0,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
					Ops: []memdx.LookupInOp{
						{
							Op: memdx.LookupInOpTypeGetDoc,
						},
					},
				}, func(resp *memdx.LookupInResponse, err error) {
					cb(resp, err)
				})
			},
			IsReadOp: true,
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					CollectionID: 0,
					Key:          key,
					VbucketID:    defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	lockResp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.GetAndLock, cli, &memdx.GetAndLockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		LockTime:     30,
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			require.NoError(tt, err)

			err = <-wait
			if !test.IsReadOp {
				// writes should fail with document locked error
				require.ErrorIs(tt, err, memdx.ErrDocLocked)
			} else {
				if testutilsint.IsOlderServerVersion(t, "8.0.0") {
					if test.Name == "LookupIn" {
						// On server version before 8.0.0, LookupIn returns document locked
						// error when trying to read a locked document.
						require.ErrorIs(tt, err, memdx.ErrDocLocked)
						return
					}
				}

				// reads should succeed
				require.NoError(tt, err)
			}
		})
	}

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Unlock, cli, &memdx.UnlockRequest{
		CollectionID: 0,
		Key:          key,
		Cas:          lockResp.Cas,
		VbucketID:    defaultTestVbucketID,
	})
	require.NoError(t, err)

}

func TestOpsCrudDocExists(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: memdx.SubdocDocFlagAddDoc, // perform the mutation as an insert
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, memdx.ErrDocExists)
		})
	}
}

func TestOpsCrudCollectionCasMismatch(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     key,
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:       key,
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "SetMeta",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.SetMeta(cli, &memdx.SetMetaRequest{
					Key:       key,
					Value:     []byte("value"),
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "DeleteMeta",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.DeleteMeta(cli, &memdx.DeleteMetaRequest{
					Key:       key,
					Cas:       1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DeleteMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Cas:       1,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: 0x01, // perform the mutation as a set
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateInDefaultFlags",
			Op: func(opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Cas:       1,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: 0, // no flags should just passthrough, server will treat as set
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, memdx.ErrCasMismatch)
		})
	}
}

func TestOpsCrudGetAndLockUnlock(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	res, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.GetAndLock, cli, &memdx.GetAndLockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
	})
	require.NoError(t, err)

	assert.Equal(t, value, res.Value)
	assert.NotZero(t, res.Cas)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Unlock, cli, &memdx.UnlockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Cas:          res.Cas,
	})
	require.NoError(t, err)

	// check what happens if we unlock an unlocked document
	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Unlock, cli, &memdx.UnlockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Cas:          res.Cas,
	})
	if testutilsint.IsOlderServerVersion(t, "7.6.0") {
		require.ErrorIs(t, err, memdx.ErrTmpFail)
	} else {
		require.ErrorIs(t, err, memdx.ErrDocNotLocked)
	}
}

func TestOpsCrudGetAndLockUnlockWrongCas(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	res, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.GetAndLock, cli, &memdx.GetAndLockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
	})
	require.NoError(t, err)

	assert.Equal(t, value, res.Value)
	assert.NotZero(t, res.Cas)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Unlock, cli, &memdx.UnlockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Cas:          222,
	})
	require.ErrorIs(t, err, memdx.ErrCasMismatch)
}

func TestOpsCrudTouch(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	res, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Touch, cli, &memdx.TouchRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Expiry:       1,
	})
	require.NoError(t, err)

	assert.NotZero(t, res.Cas)
}

func TestOpsCrudGetRandom(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
	})
	require.NoError(t, err)

	var resOut *memdx.GetRandomResponse
	require.Eventually(t, func() bool {
		res, err := memdx.SyncUnaryCall(memdx.OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, memdx.OpsCrud.GetRandom, cli, &memdx.GetRandomRequest{})
		if err != nil {
			return false
		}

		resOut = res
		return true
	}, 15*time.Second, 100*time.Millisecond)
	assert.NotZero(t, resOut.Key)
	assert.NotZero(t, resOut.Value)
	assert.NotZero(t, resOut.Cas)

}

func TestOpsCrudMutationTokens(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name            string
		SkipDocCreation bool
	}

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// {	TODO(chvck): this probably needs the doc to be locked first?
		// 	Name: "Unlock",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.Unlock(cli, &UnlockRequest{
		// 			Key:       key,
		// 			Cas:       cas,
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *UnlockResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.IncrementResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		{
			Name: "Decrement",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DecrementResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		// { TODO(chvck): this is adament it doesn't want to work.
		// 	Name: "SetMeta",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.SetMeta(cli, &SetMetaRequest{
		// 			Key:   key,
		// 			Value: []byte("value"),
		// 			Cas:       cas, // For some reason Cas is required here.
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *SetMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if !test.SkipDocCreation {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     []byte("{\"key\": \"value\"}"),
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			res := <-waitres

			elem := reflect.ValueOf(res).Elem()
			mutationToken := elem.FieldByName("MutationToken").Interface().(memdx.MutationToken)
			assert.NotZero(tt, mutationToken.VbUuid)
			assert.NotZero(tt, mutationToken.SeqNo)
		})
	}
}

func TestOpsCrudMutations(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name            string
		SkipDocCreation bool
		ExpectDeleted   bool
		ExpectedValue   []byte
	}

	usualExpectedValue := []byte(`{"key":"value2"}`)
	initialValue := []byte(`{"key":"value"}`)

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		// {	TODO(chvck): this probably needs the doc to be locked first?
		// 	Name: "Unlock",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.Unlock(cli, &UnlockRequest{
		// 			Key:       key,
		// 			Cas:       cas,
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *UnlockResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "Increment",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:       key,
					Initial:   1,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &memdx.IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.IncrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		{
			Name: "Decrement",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:       key,
					Initial:   5,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &memdx.DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.DecrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		// { TODO(chvck): this is adament it doesn't want to work.
		// 	Name: "SetMeta",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.SetMeta(cli, &SetMetaRequest{
		// 			Key:   key,
		// 			Value: []byte("value"),
		// 			Cas:       cas, // For some reason Cas is required here.
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *SetMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			VbucketID: defaultTestVbucketID,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// 	ExpectDeleted: true,
		// },
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if !test.SkipDocCreation {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			<-waitres

			getRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, memdx.OpsCrud.Get, cli, &memdx.GetRequest{
				Key:       key,
				VbucketID: defaultTestVbucketID,
			})

			if test.ExpectDeleted {
				assert.ErrorIs(t, err, memdx.ErrDocNotFound)
			} else {
				require.NoError(t, err)

				elem := reflect.ValueOf(getRes).Elem()
				value := elem.FieldByName("Value").Bytes()
				assert.Equal(tt, test.ExpectedValue, value)
			}

		})
	}
}

func TestOpsCrudMutationsSnappy(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name            string
		SkipDocCreation bool
		ExpectDeleted   bool
		ExpectedValue   []byte
	}

	usualExpectedValue := []byte(`{"key":"value2"}`)
	usualSnappyValue := snappy.Encode(nil, usualExpectedValue)
	initialValue := []byte(`{"key":"value"}`)

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     usualSnappyValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     usualSnappyValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     usualSnappyValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     usualSnappyValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     usualSnappyValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if !test.SkipDocCreation {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			<-waitres

			getRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, memdx.OpsCrud.Get, cli, &memdx.GetRequest{
				Key:       key,
				VbucketID: defaultTestVbucketID,
			})

			require.NoError(t, err)
			assert.Equal(tt, test.ExpectedValue, getRes.Value)
		})
	}
}

func TestOpsCrudMutationsSnappyBadInflate(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name            string
		SkipDocCreation bool
		ExpectDeleted   bool
		ExpectedValue   []byte
	}

	usualExpectedValue := []byte(`{"key":"value2"}`)
	initialValue := []byte(`{"key":"value"}`)

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     usualExpectedValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     usualExpectedValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     usualExpectedValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     usualExpectedValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     usualExpectedValue,
					Datatype:  uint8(memdx.DatatypeFlagCompressed),
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if !test.SkipDocCreation {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			err = <-waiterr
			require.ErrorIs(tt, err, memdx.ErrInvalidArgument)
			errType := memdx.ParseInvalidArgsError(err)
			require.Equal(t, memdx.InvalidArgsErrorCannotInflate, errType)

			<-waitres
		})
	}
}

func TestOpsCrudLookupinPathNotFound(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	resp, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.LookupIn, cli, &memdx.LookupInRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Ops: []memdx.LookupInOp{
			{
				Op:   memdx.LookupInOpTypeGet,
				Path: []byte("idontexist"),
			},
		},
	})
	require.NoError(t, err)
	require.ErrorIs(t, resp.Ops[0].Err, memdx.ErrSubDocPathNotFound)
}

func TestOpsCrudMutationsDurabilityLevel(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name            string
		SkipDocCreation bool
		ExpectDeleted   bool
		ExpectedValue   []byte
	}

	usualExpectedValue := []byte(`{"key":"value2"}`)
	initialValue := []byte(`{"key":"value"}`)

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Delete",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:             key,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "Increment",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:             key,
					Initial:         1,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &memdx.IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.IncrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		{
			Name: "Decrement",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:             key,
					Initial:         5,
					VbucketID:       defaultTestVbucketID,
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &memdx.DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.DecrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
					DurabilityLevel: memdx.DurabilityLevelMajority,
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},

		{
			Name: "SetTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "DeleteTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:                    key,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "AddTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "ReplaceTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "AppendTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "PrependTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "IncrementTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:                    key,
					Initial:                1,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &memdx.IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.IncrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		{
			Name: "DecrementTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:                    key,
					Initial:                5,
					VbucketID:              defaultTestVbucketID,
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &memdx.DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: defaultTestVbucketID,
					}, func(response *memdx.DecrementResponse, err error) {
						cb(resp, err)
					})
					if err != nil {
						cb(nil, err)
					}
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   []byte("3"),
		},
		{
			Name: "MutateInTimeout",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
					DurabilityLevel:        memdx.DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if !test.SkipDocCreation {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
				DurabilityEnabled:  true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			<-waitres

			getRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
				DurabilityEnabled:  true,
			}, memdx.OpsCrud.Get, cli, &memdx.GetRequest{
				Key:       key,
				VbucketID: defaultTestVbucketID,
			})

			if test.ExpectDeleted {
				assert.ErrorIs(t, err, memdx.ErrDocNotFound)
			} else {
				require.NoError(t, err)

				elem := reflect.ValueOf(getRes).Elem()
				value := elem.FieldByName("Value").Bytes()
				assert.Equal(tt, test.ExpectedValue, value)
			}

		})
	}
}

func TestOpsCrudLookupInErrorCases(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)
	initialValue := json.RawMessage(`{"key":"value"}`)
	//
	makeGetSubdocOp := func() memdx.LookupInOp {
		return memdx.LookupInOp{
			Op:   memdx.LookupInOpTypeGet,
			Path: []byte("key"),
		}
	}

	doSetOp := func(tt *testing.T, key []byte) {
		_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
			Key:       key,
			VbucketID: defaultTestVbucketID,
			Value:     initialValue,
			Datatype:  uint8(0x01),
		})
		require.NoError(tt, err)
	}

	type test struct {
		Name          string
		Request       *memdx.LookupInRequest
		ExpectedError error
		IsIndexLevel  bool
		RunFirst      func(*testing.T, []byte)
	}

	tests := []test{
		{
			Name: "KeyNotFound",
			Request: &memdx.LookupInRequest{
				Ops: []memdx.LookupInOp{
					makeGetSubdocOp(),
				},
			},
			ExpectedError: memdx.ErrDocNotFound,
		},
		{
			Name: "InvalidCombo",
			Request: &memdx.LookupInRequest{
				Ops: []memdx.LookupInOp{
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
					makeGetSubdocOp(),
				},
			},
			ExpectedError: memdx.ErrSubDocInvalidCombo,
			RunFirst:      doSetOp,
		},
		{
			Name: "InvalidXattrOrder",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					makeGetSubdocOp(),
					{
						Op:    memdx.LookupInOpTypeGet,
						Path:  []byte("key"),
						Flags: memdx.SubdocOpFlagXattrPath,
					},
				},
			},
			ExpectedError: memdx.ErrSubDocInvalidXattrOrder,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "InvalidXattrFlagCombo",
		// 	Request: &LookupInRequest{
		// 		VbucketID: defaultTestVbucketID,
		// 		Ops: []LookupInOp{
		// 			{
		// 				Op:    LookupInOpTypeGet,
		// 				Path:  []byte("key"),
		// 				Flags: SubdocOpFlagExpandMacros,
		// 			},
		// 			makeGetSubdocOp(),
		// 		},
		// 	},
		// 	ExpectedError: ErrSubDocXattrInvalidFlagCombo,
		// 	RunFirst:      doSetOp,
		// },

		// Path level errors

		{
			Name: "DocTooDeep",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					makeGetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocDocTooDeep,
			RunFirst: func(tt *testing.T, key []byte) {
				value := map[string]interface{}{
					"key": map[string]interface{}{},
				}
				v := value["key"].(map[string]interface{})
				for i := 0; i < 35; i++ {
					v["key"] = map[string]interface{}{}
					v = v["key"].(map[string]interface{})
				}

				b, err := json.Marshal(value)
				require.NoError(tt, err)

				_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     b,
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocNotJSON",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					makeGetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocNotJSON,
			RunFirst: func(tt *testing.T, key []byte) {
				_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     []byte("imnotjson"),
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocPathNotFound",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					{
						Op:   memdx.LookupInOpTypeGet,
						Path: []byte("idontexist"),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathNotFound,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathMismatch",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					{
						Op:   memdx.LookupInOpTypeGet,
						Path: []byte("key[9]"),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathInvalid",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					{
						Op:   memdx.LookupInOpTypeGet,
						Path: []byte("key["),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathInvalid,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocPathTooBig",
		// 	Request: &LookupInRequest{
		// 		VbucketID: defaultTestVbucketID,
		// 		Ops: []LookupInOp{
		// 			{
		// 				Op:   LookupInOpTypeGet,
		// 				Path: []byte("key["),
		// 			},
		// 		},
		// 	},
		// 	IsIndexLevel:  true,
		// 	ExpectedError: ErrSubDocPathInvalid,
		// 	RunFirst:      doSetOp,
		// },
		{
			Name: "SubDocUnknownVattr",
			Request: &memdx.LookupInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.LookupInOp{
					{
						Op:    memdx.LookupInOpTypeGet,
						Path:  []byte("$nonsense"),
						Flags: memdx.SubdocOpFlagXattrPath,
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocXattrUnknownVAttr,
			RunFirst:      doSetOp,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			key := []byte(uuid.NewString()[:6])

			if test.RunFirst != nil {
				test.RunFirst(tt, key)
			}

			req := test.Request
			req.Key = key
			req.VbucketID = 1

			res, err := memdx.SyncUnaryCall(
				memdx.OpsCrud{
					ExtFramesEnabled:      true,
					CollectionsEnabled:    true,
					DurabilityEnabled:     true,
					PreserveExpiryEnabled: true,
				},
				memdx.OpsCrud.LookupIn,
				cli,
				req,
			)

			if test.IsIndexLevel {
				require.NoError(tt, err)

				err := res.Ops[0].Err
				require.ErrorIs(tt, err, test.ExpectedError)

				var subDocErr *memdx.SubDocError
				if assert.ErrorAs(tt, err, &subDocErr) {
					// Not really sure if this is testing anything due to zero values.
					assert.Equal(tt, 0, subDocErr.OpIndex)
				}
			} else {
				require.ErrorIs(tt, err, test.ExpectedError)
			}
		})
	}
}

func TestOpsCrudLookupInMultipleErrorAndSuccess(t *testing.T) {
	path1 := []byte("value")
	path3 := []byte("value3")
	dispatcher := &testCrudDispatcher{
		Pak: &memdx.Packet{
			Status: memdx.StatusSubDocMultiPathFailure,
		},
	}
	val := make([]byte, len(path1)+len(path3)+18)
	binary.BigEndian.PutUint16(val[:], uint16(memdx.StatusSuccess))
	binary.BigEndian.PutUint32(val[2:], uint32(len(path1)))
	copy(val[6:], path1)
	binary.BigEndian.PutUint16(val[len(path1)+6:], uint16(memdx.StatusSubDocPathNotFound))
	binary.BigEndian.PutUint32(val[len(path1)+8:], 0)
	binary.BigEndian.PutUint16(val[len(path1)+12:], uint16(memdx.StatusSuccess))
	binary.BigEndian.PutUint32(val[len(path1)+14:], uint32(len(path3)))
	copy(val[len(path1)+18:], path3)

	dispatcher.Pak.Value = val

	res, err := memdx.SyncUnaryCall(
		memdx.OpsCrud{
			ExtFramesEnabled:      true,
			CollectionsEnabled:    true,
			DurabilityEnabled:     true,
			PreserveExpiryEnabled: true,
		},
		memdx.OpsCrud.LookupIn,
		dispatcher,
		&memdx.LookupInRequest{
			Key:       []byte(uuid.NewString()[:6]),
			VbucketID: defaultTestVbucketID,
			Ops: []memdx.LookupInOp{
				{
					Op:   memdx.LookupInOpTypeGet,
					Path: []byte("key"),
				},
				{
					Op:   memdx.LookupInOpTypeGet,
					Path: []byte("key2"),
				},
				{
					Op:   memdx.LookupInOpTypeGet,
					Path: []byte("key3"),
				},
			},
		},
	)
	require.NoError(t, err)

	require.Len(t, res.Ops, 3)

	assert.Equal(t, path1, res.Ops[0].Value)
	assert.ErrorIs(t, res.Ops[1].Err, memdx.ErrSubDocPathNotFound)
	assert.Equal(t, path3, res.Ops[2].Value)
}

func TestOpsCrudMutateInErrorCases(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)
	initialValue := json.RawMessage(`{"key":"value"}`)

	makeSetSubdocOp := func() memdx.MutateInOp {
		return memdx.MutateInOp{
			Op:    memdx.MutateInOpTypeDictSet,
			Path:  []byte("key"),
			Value: []byte(`"value"`),
		}
	}

	doSetOp := func(tt *testing.T, key []byte) {
		_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
			Key:       key,
			VbucketID: defaultTestVbucketID,
			Value:     initialValue,
			Datatype:  uint8(0x01),
		})
		require.NoError(tt, err)
	}

	type test struct {
		Name          string
		Request       *memdx.MutateInRequest
		ExpectedError error
		IsIndexLevel  bool
		RunFirst      func(*testing.T, []byte)
	}

	tests := []test{
		{
			Name: "DocNotFound",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
			},
			ExpectedError: memdx.ErrDocNotFound,
		},
		{
			Name: "DocExists",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
				Flags: memdx.SubdocDocFlagAddDoc,
			},
			ExpectedError: memdx.ErrDocExists,
			RunFirst:      doSetOp,
		},
		{
			Name: "CasMismatch",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
				Cas: 123455,
			},
			ExpectedError: memdx.ErrCasMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "InvalidCombo",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
					makeSetSubdocOp(),
				},
				Flags: memdx.SubdocDocFlagAddDoc,
			},
			ExpectedError: memdx.ErrSubDocInvalidCombo,
		},
		{
			Name: "InvalidXattrOrder",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
					{
						Op:    memdx.MutateInOpTypeDictSet,
						Path:  []byte("key"),
						Value: []byte("value"),
						Flags: memdx.SubdocOpFlagXattrPath,
					},
				},
				Flags: memdx.SubdocDocFlagAddDoc,
			},
			ExpectedError: memdx.ErrSubDocInvalidXattrOrder,
		},
		{
			Name: "XattrUnknownMacro",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeDictSet,
						Path:  []byte("key"),
						Value: []byte("${something}"),
						Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					},
				},
				Flags: memdx.SubdocDocFlagAddDoc,
			},
			ExpectedError: memdx.ErrSubDocXattrUnknownMacro,
		},
		{
			Name: "XattrCannotModifyVattr",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeDictSet,
						Path:  []byte("$document"),
						Value: []byte("value"),
						Flags: memdx.SubdocOpFlagXattrPath | memdx.SubdocOpFlagExpandMacros,
					},
				},
				Flags: memdx.SubdocDocFlagAddDoc,
			},
			ExpectedError: memdx.ErrSubDocXattrCannotModifyVAttr,
		},
		{
			Name: "CanOnlyReviveDeletedDocuments",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
				Flags: memdx.SubdocDocFlagReviveDocument | memdx.SubdocDocFlagAccessDeleted,
			},
			ExpectedError: memdx.ErrSubDocCanOnlyReviveDeletedDocuments,
			RunFirst:      doSetOp,
		},
		// {
		// Name: "DeletedDocumentCantHaveValue",
		// Request: &MutateInRequest{
		// 	VbucketID: defaultTestVbucketID,
		// 	Ops: []MutateInOp{
		// 		makeSetSubdocOp(),
		// 	},
		// 	Flags: SubdocDocFlagCreateAsDeleted | SubdocDocFlagAccessDeleted | SubdocDocFlagAddDoc,
		// },
		// ExpectedError: ErrSubDocCanOnlyReviveDeletedDocuments,
		// },

		// Path level errors
		{
			Name: "DocTooDeep",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocDocTooDeep,
			RunFirst: func(tt *testing.T, key []byte) {
				value := map[string]interface{}{
					"key": map[string]interface{}{},
				}
				v := value["key"].(map[string]interface{})
				for i := 0; i < 35; i++ {
					v["key"] = map[string]interface{}{}
					v = v["key"].(map[string]interface{})
				}

				b, err := json.Marshal(value)
				require.NoError(tt, err)

				_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     b,
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocNotJSON",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					makeSetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocNotJSON,
			RunFirst: func(tt *testing.T, key []byte) {
				_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     []byte("imnotjson"),
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocPathNotFound",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeReplace,
						Path:  []byte("idontexit"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathNotFound,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathMismatch",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeDictSet,
						Path:  []byte("key[0]"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathInvalid",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeDictSet,
						Path:  []byte("key["),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathInvalid,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocPathTooBig",
		// 	Request: &MutateInRequest{
		// 		VbucketID: defaultTestVbucketID,
		// 		Ops: []MutateInOp{
		// 			{
		// 				Op:    MutateInOpTypeDictSet,
		// 				Path:  []byte("key"),
		// 				Value: []byte(`"value"`),
		// 			},
		// 		},
		// 	},
		// 	IsIndexLevel:  true,
		// 	ExpectedError: ErrSubDocPathInvalid,
		// 	RunFirst:      doSetOp,
		// },
		// {
		// 	Name: "SubDocBadRange",
		// 	Request: &MutateInRequest{
		// 		VbucketID: defaultTestVbucketID,
		// 		Ops: []MutateInOp{
		// 			{
		// 				Op:    MutateInOpTypeCounter,
		// 				Path:  []byte("key"),
		// 				Value: []byte("120000000000000000000000000"),
		// 			},
		// 		},
		// 		Flags: SubdocDocFlagMkDoc,
		// 	},
		// 	IsIndexLevel:  true,
		// 	ExpectedError: ErrSubDocBadRange,
		// },
		{
			Name: "SubDocBadDelta",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeCounter,
						Path:  []byte("key"),
						Value: []uint8{1},
					},
				},
				Flags: memdx.SubdocDocFlagMkDoc,
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocBadDelta,
		},
		{
			Name: "SubDocPathExists",
			Request: &memdx.MutateInRequest{
				VbucketID: defaultTestVbucketID,
				Ops: []memdx.MutateInOp{
					{
						Op:    memdx.MutateInOpTypeDictAdd,
						Path:  []byte("key"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: memdx.ErrSubDocPathExists,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocValueTooDeep",
		// 	Request: &MutateInRequest{
		// 		VbucketID: defaultTestVbucketID,
		// 		Ops: []MutateInOp{
		// 			{
		// 				Op:    MutateInOpTypeCounter,
		// 				Path:  []byte("key"),
		// 				Value: []byte("120000000000000000000000000"),
		// 			},
		// 		},
		// 		Flags: SubdocDocFlagMkDoc,
		// 	},
		// 	IsIndexLevel:  true,
		// 	ExpectedError: ErrSubDocBadRange,
		// },
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			key := []byte(uuid.NewString()[:6])

			if test.RunFirst != nil {
				test.RunFirst(tt, key)
			}

			req := test.Request
			req.Key = key

			_, err := memdx.SyncUnaryCall(
				memdx.OpsCrud{
					ExtFramesEnabled:      true,
					CollectionsEnabled:    true,
					DurabilityEnabled:     true,
					PreserveExpiryEnabled: true,
				},
				memdx.OpsCrud.MutateIn,
				cli,
				req,
			)
			require.ErrorIs(tt, err, test.ExpectedError)

			if test.IsIndexLevel {
				var subDocErr *memdx.SubDocError
				if assert.ErrorAs(tt, err, &subDocErr) {
					// Not really sure if this is testing anything due to zero values.
					assert.Equal(tt, 0, subDocErr.OpIndex)
				}
			}
		})
	}
}

func TestOpsCrudValueTooLarge(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op             func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name           string
		CreateDocFirst bool
	}

	var val []byte
	for i := 0; i < 21000000; i++ {
		val = append(val, byte(i))
	}

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Set(cli, &memdx.SetRequest{
					Key:       key,
					Value:     val,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Add",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Add(cli, &memdx.AddRequest{
					Key:       key,
					Value:     val,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AddResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     val,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			CreateDocFirst: true,
		},
		{
			Name: "Append",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     val,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
			CreateDocFirst: true,
		},
		{
			Name: "Prepend",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     val,
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
			CreateDocFirst: true,
		},
		{
			Name: "SetMeta",
			Op: func(opsCrud memdx.OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.SetMeta(cli, &memdx.SetMetaRequest{
					Key:       key,
					Value:     val,
					Cas:       cas, // For some reason Cas is required here.
					VbucketID: defaultTestVbucketID,
				}, func(resp *memdx.SetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			waiterr := make(chan error, 1)
			waitres := make(chan interface{}, 1)

			key := []byte(uuid.NewString())

			var cas uint64
			if test.CreateDocFirst {
				setRes, err := memdx.SyncUnaryCall(memdx.OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Value:     []byte(""),
					Datatype:  uint8(0x00),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.ErrorIs(tt, <-waiterr, memdx.ErrValueTooLarge)
		})
	}
}

func TestOpsCrudOnBehalfOf(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op   func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error)
		Name string
	}

	/*
		Since it isn't possible to inspect what particular user an operation was executed
		under, we test on-behalf-of execution by setting the user to something invalid and
		then validating that the operation is rejected with access denied.
	*/

	tests := []test{
		{
			Name: "Get",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Get(cli, &memdx.GetRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.GetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndLock",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndLock(cli, &memdx.GetAndLockRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &memdx.GetAndTouchRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Unlock",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Unlock(cli, &memdx.UnlockRequest{
					Key:       key,
					Cas:       1,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.UnlockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Touch(cli, &memdx.TouchRequest{
					Key:       key,
					Expiry:    60,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Delete(cli, &memdx.DeleteRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Replace(cli, &memdx.ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Append(cli, &memdx.AppendRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Prepend(cli, &memdx.PrependRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Increment(cli, &memdx.IncrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.Decrement(cli, &memdx.DecrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.GetMeta(cli, &memdx.GetMetaRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "LookupIn",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.LookupIn(cli, &memdx.LookupInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.LookupInOp{
						{
							Op:   memdx.LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.LookupInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(key []byte, opsCrud memdx.OpsCrud, cb func(interface{}, error)) (memdx.PendingOp, error) {
				return opsCrud.MutateIn(cli, &memdx.MutateInRequest{
					Key:       key,
					VbucketID: defaultTestVbucketID,
					Ops: []memdx.MutateInOp{
						{
							Op:    memdx.MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					CrudRequestMeta: memdx.CrudRequestMeta{
						OnBehalfOf: "invalid-user",
					},
				}, func(resp *memdx.MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			key := []byte(uuid.NewString()[:6])

			_, err := test.Op(key, memdx.OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, func(i interface{}, err error) {
				wait <- err
			})
			if !assert.NoError(tt, err) {
				return
			}

			assert.ErrorIs(tt, <-wait, memdx.ErrAccessError)
		})
	}
}

func TestOpsCrudLookupinMultiXattr(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        []byte(`{"key":"value"}`),
		Datatype:     uint8(0x01),
		Expiry:       60,
	})
	require.NoError(t, err)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.LookupIn, cli, &memdx.LookupInRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("key"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Path:  []byte("key2"),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:   memdx.LookupInOpTypeGet,
				Path: []byte("key"),
			},
		},
	})

	if testutilsint.IsOlderServerVersion(t, "7.6.0") {
		require.ErrorIs(t, err, memdx.ErrSubDocXattrInvalidKeyCombo)
	} else {
		require.NoError(t, err)
	}
}

func TestOpsCrudMutateinMultiXattr(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.MutateIn, cli, &memdx.MutateInRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Ops: []memdx.MutateInOp{
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("key"),
				Value: []byte(`"value"`),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("key2"),
				Value: []byte(`"value"`),
				Flags: memdx.SubdocOpFlagXattrPath,
			},
			{
				Op:    memdx.MutateInOpTypeDictSet,
				Path:  []byte("key"),
				Value: []byte(`"value"`),
			},
		},
		Flags: memdx.SubdocDocFlagAddDoc,
	})

	if testutilsint.IsOlderServerVersion(t, "7.6.0") {
		require.ErrorIs(t, err, memdx.ErrSubDocXattrInvalidKeyCombo)
	} else {
		require.NoError(t, err)
	}
}

func TestOpsCrudCounterNonNumericDoc(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	key := []byte(uuid.NewString())

	cli := createTestClient(t)

	_, err := memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Set, cli, &memdx.SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Value:        []byte(`{"key":"value"}`),
		Datatype:     uint8(0x01),
	})
	require.NoError(t, err)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Increment, cli, &memdx.IncrementRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Delta:        1,
	})
	require.ErrorIs(t, err, memdx.ErrDeltaBadval)

	_, err = memdx.SyncUnaryCall(memdx.OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, memdx.OpsCrud.Decrement, cli, &memdx.DecrementRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    defaultTestVbucketID,
		Delta:        1,
	})
	require.ErrorIs(t, err, memdx.ErrDeltaBadval)
}

type testCrudDispatcher struct {
	Pak *memdx.Packet
}

func (t *testCrudDispatcher) Dispatch(packet *memdx.Packet, callback memdx.DispatchCallback) (memdx.PendingOp, error) {
	go func() {
		callback(t.Pak, nil)
	}()

	return memdx.PendingOpNoop{}, nil
}

func (t *testCrudDispatcher) LocalAddr() string {
	return "localaddr"
}

func (t *testCrudDispatcher) RemoteAddr() string {
	return "remoteaddr"
}
