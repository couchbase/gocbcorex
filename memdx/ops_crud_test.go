package memdx

import (
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpsCrudGets(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
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
		{
			Name: "GetMeta",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetMeta(cli, &GetMetaRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
			CheckOverride: func(t *testing.T, res interface{}) {
				randRes, ok := res.(*GetMetaResponse)
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
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.LookupIn(cli, &LookupInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []LookupInOp{
						{
							Op:   LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *LookupInResponse, err error) {
					cb(resp, err)
				})
			},
			CheckOverride: func(t *testing.T, res interface{}) {
				randRes, ok := res.(*LookupInResponse)
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

	_, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Set, cli, &SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
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

func TestOpsCrudKeyNotFound(t *testing.T) {
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op   func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Get",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
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
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndLock(cli, &GetAndLockRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetAndLockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetAndTouch",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndTouch(cli, &GetAndTouchRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetAndTouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Unlock",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Unlock(cli, &UnlockRequest{
					Key:       key,
					Cas:       1,
					VbucketID: 1,
				}, func(resp *UnlockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Touch(cli, &TouchRequest{
					Key:       key,
					Expiry:    60,
					VbucketID: 1,
				}, func(resp *TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: 1,
				}, func(resp *IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					Key:       key,
					Initial:   uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID: 1,
				}, func(resp *DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetMeta(cli, &GetMetaRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// Server is always responding with exists
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			Options:   0x08, // SKIP_CONFLICT_RESOLUTION_FLAG
		// 			VbucketID: 1,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "LookupIn",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.LookupIn(cli, &LookupInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []LookupInOp{
						{
							Op:   LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *LookupInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(key []byte, opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
				}, func(resp *MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(tt *testing.T) {
			wait := make(chan error, 1)

			key := []byte(uuid.NewString()[:6])

			_, err := test.Op(key, OpsCrud{
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

func TestOpsCrudCollectionNotKnown(t *testing.T) {
	testutils.SkipIfShortTest(t)

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
		// The server is hanging on this request.
		// {
		// 	Name: "GetRandom",
		// 	Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.GetRandom(cli, &GetRandomRequest{
		// 			CollectionID: 2222,
		// 		}, func(resp *GetRandomResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "GetAndLock",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetAndLock(cli, &GetAndLockRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
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
					VbucketID:    1,
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
					VbucketID:    1,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Unlock",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Unlock(cli, &UnlockRequest{
					CollectionID: 2222,
					Key:          key,
					Cas:          1,
					VbucketID:    1,
				}, func(resp *UnlockResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Touch",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Touch(cli, &TouchRequest{
					CollectionID: 2222,
					Key:          key,
					Expiry:       60,
					VbucketID:    1,
				}, func(resp *TouchResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Add",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    1,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    1,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    1,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					VbucketID:    1,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					CollectionID: 2222,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID:    1,
				}, func(resp *IncrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Decrement",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					CollectionID: 2222,
					Key:          key,
					Initial:      uint64(0xFFFFFFFFFFFFFFFF),
					VbucketID:    1,
				}, func(resp *DecrementResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "GetMeta",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.GetMeta(cli, &GetMetaRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
				}, func(resp *GetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "SetMeta",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.SetMeta(cli, &SetMetaRequest{
					CollectionID: 2222,
					Key:          key,
					Value:        []byte("value"),
					Cas:          1, // For some reason Cas is required here.
					VbucketID:    1,
				}, func(resp *SetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
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
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.LookupIn(cli, &LookupInRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
					Ops: []LookupInOp{
						{
							Op:   LookupInOpTypeGet,
							Path: []byte("key"),
						},
					},
				}, func(resp *LookupInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					CollectionID: 2222,
					Key:          key,
					VbucketID:    1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
				}, func(resp *MutateInResponse, err error) {
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

func TestOpsCrudDocExists(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Add",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: SubdocDocFlagAddDoc, // perform the mutation as an insert
				}, func(resp *MutateInResponse, err error) {
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
		Expiry:       60,
	})
	require.NoError(t, err)

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

			assert.ErrorIs(tt, <-wait, ErrDocExists)
		})
	}
}

func TestOpsCrudCollectionCasMismatch(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	type test struct {
		Op   func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error)
		Name string
	}

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					Key:       key,
					Value:     key,
					Cas:       1,
					VbucketID: 1,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Delete",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:       key,
					Cas:       1,
					VbucketID: 1,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Replace",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					Cas:       1,
					VbucketID: 1,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "SetMeta",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.SetMeta(cli, &SetMetaRequest{
					Key:       key,
					Value:     []byte("value"),
					Cas:       1,
					VbucketID: 1,
				}, func(resp *SetMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "DeleteMeta",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
					Key:       key,
					Cas:       1,
					VbucketID: 1,
				}, func(resp *DeleteMetaResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateIn",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Cas:       1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: 0x01, // perform the mutation as a set
				}, func(resp *MutateInResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "MutateInDefaultFlags",
			Op: func(opsCrud OpsCrud, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Cas:       1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("value"),
						},
					},
					Flags: 0, // no flags should just passthrough, server will treat as set
				}, func(resp *MutateInResponse, err error) {
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
		Expiry:       60,
	})
	require.NoError(t, err)

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

			assert.ErrorIs(tt, <-wait, ErrCasMismatch)
		})
	}
}

func TestOpsCrudGetAndLockUnlock(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

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

	res, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.GetAndLock, cli, &GetAndLockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
	})
	require.NoError(t, err)

	assert.Equal(t, value, res.Value)
	assert.NotZero(t, res.Cas)

	_, err = syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Unlock, cli, &UnlockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Cas:          res.Cas,
	})
	require.NoError(t, err)
}

func TestOpsCrudGetAndLockUnlockWrongCas(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

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

	res, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.GetAndLock, cli, &GetAndLockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
	})
	require.NoError(t, err)

	assert.Equal(t, value, res.Value)
	assert.NotZero(t, res.Cas)

	_, err = syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Unlock, cli, &UnlockRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Cas:          222,
	})
	require.ErrorIs(t, err, ErrCasMismatch)
}

func TestOpsCrudTouch(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte(uuid.NewString())
	datatype := uint8(0x01)

	cli := createTestClient(t)

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

	res, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Touch, cli, &TouchRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Expiry:       1,
	})
	require.NoError(t, err)

	assert.NotZero(t, res.Cas)
}

func TestOpsCrudMutationTokens(t *testing.T) {
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error)
		Name            string
		SkipDocCreation bool
	}

	tests := []test{
		{
			Name: "Set",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					Key:       key,
					Value:     key,
					VbucketID: 1,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
		},
		// {	TODO(chvck): this probably needs the doc to be locked first?
		// 	Name: "Unlock",
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.Unlock(cli, &UnlockRequest{
		// 			Key:       key,
		// 			Cas:       cas,
		// 			VbucketID: 1,
		// 		}, func(resp *UnlockResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "Delete",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Add",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		{
			Name: "Replace",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Append",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Prepend",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					Key:       key,
					Value:     []byte("value"),
					VbucketID: 1,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
		},
		{
			Name: "Increment",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *IncrementResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		{
			Name: "Decrement",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *DecrementResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
		},
		// { TODO(chvck): this is adament it doesn't want to work.
		// 	Name: "SetMeta",
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.SetMeta(cli, &SetMetaRequest{
		// 			Key:   key,
		// 			Value: []byte("value"),
		// 			Cas:       cas, // For some reason Cas is required here.
		// 			VbucketID: 1,
		// 		}, func(resp *SetMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			VbucketID: 1,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "MutateIn",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
				}, func(resp *MutateInResponse, err error) {
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
				setRes, err := syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     []byte("{\"key\": \"value\"}"),
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(OpsCrud{
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
			mutationToken := elem.FieldByName("MutationToken").Interface().(MutationToken)
			assert.NotZero(tt, mutationToken.VbUuid)
			assert.NotZero(tt, mutationToken.SeqNo)
		})
	}
}

func TestOpsCrudMutations(t *testing.T) {
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error)
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: 1,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		// {	TODO(chvck): this probably needs the doc to be locked first?
		// 	Name: "Unlock",
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.Unlock(cli, &UnlockRequest{
		// 			Key:       key,
		// 			Cas:       cas,
		// 			VbucketID: 1,
		// 		}, func(resp *UnlockResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		{
			Name: "Delete",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:       key,
					VbucketID: 1,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "Add",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: 1,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: 1,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: 1,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					Key:       key,
					Value:     usualExpectedValue,
					VbucketID: 1,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "Increment",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					Key:       key,
					Initial:   1,
					VbucketID: 1,
				}, func(resp *IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *IncrementResponse, err error) {
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					Key:       key,
					Initial:   5,
					VbucketID: 1,
				}, func(resp *DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *DecrementResponse, err error) {
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
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.SetMeta(cli, &SetMetaRequest{
		// 			Key:   key,
		// 			Value: []byte("value"),
		// 			Cas:       cas, // For some reason Cas is required here.
		// 			VbucketID: 1,
		// 		}, func(resp *SetMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// },
		// {
		// 	Name: "DeleteMeta",
		// 	Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
		// 		return opsCrud.DeleteMeta(cli, &DeleteMetaRequest{
		// 			Key:       key,
		// 			VbucketID: 1,
		// 		}, func(resp *DeleteMetaResponse, err error) {
		// 			cb(resp, err)
		// 		})
		// 	},
		// 	ExpectDeleted: true,
		// },
		{
			Name: "MutateIn",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
				}, func(resp *MutateInResponse, err error) {
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
				setRes, err := syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, key, cas, func(i interface{}, err error) {
				waiterr <- err
				waitres <- i
			})
			require.NoError(tt, err)

			require.NoError(tt, <-waiterr)

			<-waitres

			getRes, err := syncUnaryCall(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
			}, OpsCrud.Get, cli, &GetRequest{
				Key:       key,
				VbucketID: 1,
			})

			if test.ExpectDeleted {
				assert.ErrorIs(t, err, ErrDocNotFound)
			} else {
				require.NoError(t, err)

				elem := reflect.ValueOf(getRes).Elem()
				value := elem.FieldByName("Value").Bytes()
				assert.Equal(tt, test.ExpectedValue, value)
			}

		})
	}
}

func TestOpsCrudLookupinPathNotFound(t *testing.T) {
	testutils.SkipIfShortTest(t)

	key := []byte(uuid.NewString())
	value := []byte("{\"key\": \"value\"}")
	datatype := uint8(0x01)

	cli := createTestClient(t)

	_, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.Set, cli, &SetRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Value:        value,
		Datatype:     datatype,
		Expiry:       60,
	})
	require.NoError(t, err)

	resp, err := syncUnaryCall(OpsCrud{
		CollectionsEnabled: true,
		ExtFramesEnabled:   true,
	}, OpsCrud.LookupIn, cli, &LookupInRequest{
		CollectionID: 0,
		Key:          key,
		VbucketID:    1,
		Ops: []LookupInOp{
			{
				Op:   LookupInOpTypeGet,
				Path: []byte("idontexist"),
			},
		},
	})
	require.NoError(t, err)
	require.ErrorIs(t, resp.Ops[0].Err, ErrSubDocPathNotFound)
}

func TestOpsCrudMutationsDurabilityLevel(t *testing.T) {
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)

	type test struct {
		Op              func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error)
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Delete",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:             key,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "Add",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "Replace",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "Append",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "Prepend",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					Key:             key,
					Value:           usualExpectedValue,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "Increment",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					Key:             key,
					Initial:         1,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *IncrementResponse, err error) {
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					Key:             key,
					Initial:         5,
					VbucketID:       1,
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *DecrementResponse, err error) {
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
					DurabilityLevel: DurabilityLevelMajority,
				}, func(resp *MutateInResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},

		{
			Name: "SetTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Set(cli, &SetRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *SetResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "DeleteTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Delete(cli, &DeleteRequest{
					Key:                    key,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *DeleteResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectDeleted: true,
		},
		{
			Name: "AddTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Add(cli, &AddRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *AddResponse, err error) {
					cb(resp, err)
				})
			},
			SkipDocCreation: true,
			ExpectedValue:   usualExpectedValue,
		},
		{
			Name: "ReplaceTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Replace(cli, &ReplaceRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *ReplaceResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: usualExpectedValue,
		},
		{
			Name: "AppendTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Append(cli, &AppendRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *AppendResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(initialValue, usualExpectedValue...),
		},
		{
			Name: "PrependTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Prepend(cli, &PrependRequest{
					Key:                    key,
					Value:                  usualExpectedValue,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *PrependResponse, err error) {
					cb(resp, err)
				})
			},
			ExpectedValue: append(usualExpectedValue, initialValue...),
		},
		{
			Name: "IncrementTimeout",
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Increment(cli, &IncrementRequest{
					Key:                    key,
					Initial:                1,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *IncrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Increment(cli, &IncrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *IncrementResponse, err error) {
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.Decrement(cli, &DecrementRequest{
					Key:                    key,
					Initial:                5,
					VbucketID:              1,
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *DecrementResponse, err error) {
					if err != nil {
						cb(nil, err)
					}

					_, err = opsCrud.Decrement(cli, &DecrementRequest{
						Key:       key,
						Delta:     2,
						VbucketID: 1,
					}, func(response *DecrementResponse, err error) {
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
			Op: func(opsCrud OpsCrud, key []byte, cas uint64, cb func(interface{}, error)) (PendingOp, error) {
				return opsCrud.MutateIn(cli, &MutateInRequest{
					Key:       key,
					VbucketID: 1,
					Ops: []MutateInOp{
						{
							Op:    MutateInOpTypeDictSet,
							Path:  []byte("key"),
							Value: []byte("\"value2\""),
						},
					},
					DurabilityLevel:        DurabilityLevelMajority,
					DurabilityLevelTimeout: 10 * time.Second,
				}, func(resp *MutateInResponse, err error) {
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
				setRes, err := syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     initialValue,
					Datatype:  uint8(0x01),
				})
				require.NoError(t, err)
				cas = setRes.Cas
			}

			_, err := test.Op(OpsCrud{
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

			getRes, err := syncUnaryCall(OpsCrud{
				CollectionsEnabled: true,
				ExtFramesEnabled:   true,
				DurabilityEnabled:  true,
			}, OpsCrud.Get, cli, &GetRequest{
				Key:       key,
				VbucketID: 1,
			})

			if test.ExpectDeleted {
				assert.ErrorIs(t, err, ErrDocNotFound)
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
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)
	initialValue := json.RawMessage(`{"key":"value"}`)
	//
	makeGetSubdocOp := func() LookupInOp {
		return LookupInOp{
			Op:   LookupInOpTypeGet,
			Path: []byte("key"),
		}
	}

	doSetOp := func(tt *testing.T, key []byte) {
		_, err := syncUnaryCall(OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, OpsCrud.Set, cli, &SetRequest{
			Key:       key,
			VbucketID: 1,
			Value:     initialValue,
			Datatype:  uint8(0x01),
		})
		require.NoError(tt, err)
	}

	type test struct {
		Name          string
		Request       *LookupInRequest
		ExpectedError error
		IsIndexLevel  bool
		RunFirst      func(*testing.T, []byte)
	}

	tests := []test{
		{
			Name: "KeyNotFound",
			Request: &LookupInRequest{
				Ops: []LookupInOp{
					makeGetSubdocOp(),
				},
			},
			ExpectedError: ErrDocNotFound,
		},
		{
			Name: "InvalidCombo",
			Request: &LookupInRequest{
				Ops: []LookupInOp{
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
			ExpectedError: ErrSubDocInvalidCombo,
			RunFirst:      doSetOp,
		},
		{
			Name: "InvalidXattrOrder",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					makeGetSubdocOp(),
					{
						Op:    LookupInOpTypeGet,
						Path:  []byte("key"),
						Flags: SubdocOpFlagXattrPath,
					},
				},
			},
			ExpectedError: ErrSubDocInvalidXattrOrder,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "InvalidXattrFlagCombo",
		// 	Request: &LookupInRequest{
		// 		VbucketID: 1,
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
		{
			Name: "InvalidXattrKeyCombo",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					{
						Op:    LookupInOpTypeGet,
						Path:  []byte("key"),
						Flags: SubdocOpFlagXattrPath,
					},
					{
						Op:    LookupInOpTypeGet,
						Path:  []byte("key2"),
						Flags: SubdocOpFlagXattrPath,
					},
					makeGetSubdocOp(),
				},
			},
			ExpectedError: ErrSubDocXattrInvalidKeyCombo,
			RunFirst:      doSetOp,
		},

		// Path level errors

		{
			Name: "DocTooDeep",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					makeGetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocDocTooDeep,
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

				_, err = syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     b,
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocNotJSON",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					makeGetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocNotJSON,
			RunFirst: func(tt *testing.T, key []byte) {
				_, err := syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     []byte("imnotjson"),
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocPathNotFound",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					{
						Op:   LookupInOpTypeGet,
						Path: []byte("idontexist"),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathNotFound,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathMismatch",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					{
						Op:   LookupInOpTypeGet,
						Path: []byte("key[9]"),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathInvalid",
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					{
						Op:   LookupInOpTypeGet,
						Path: []byte("key["),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathInvalid,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocPathTooBig",
		// 	Request: &LookupInRequest{
		// 		VbucketID: 1,
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
			Request: &LookupInRequest{
				VbucketID: 1,
				Ops: []LookupInOp{
					{
						Op:    LookupInOpTypeGet,
						Path:  []byte("$nonsense"),
						Flags: SubdocOpFlagXattrPath,
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocXattrUnknownVAttr,
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

			res, err := syncUnaryCall(
				OpsCrud{
					ExtFramesEnabled:      true,
					CollectionsEnabled:    true,
					DurabilityEnabled:     true,
					PreserveExpiryEnabled: true,
				},
				OpsCrud.LookupIn,
				cli,
				req,
			)

			if test.IsIndexLevel {
				require.NoError(tt, err)

				err := res.Ops[0].Err
				require.ErrorIs(tt, err, test.ExpectedError)

				var subDocErr *SubDocError
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
		Pak: &Packet{
			Status: StatusSubDocMultiPathFailure,
		},
	}
	val := make([]byte, len(path1)+len(path3)+18)
	binary.BigEndian.PutUint16(val[:], uint16(StatusSuccess))
	binary.BigEndian.PutUint32(val[2:], uint32(len(path1)))
	copy(val[6:], path1)
	binary.BigEndian.PutUint16(val[len(path1)+6:], uint16(StatusSubDocPathNotFound))
	binary.BigEndian.PutUint32(val[len(path1)+8:], 0)
	binary.BigEndian.PutUint16(val[len(path1)+12:], uint16(StatusSuccess))
	binary.BigEndian.PutUint32(val[len(path1)+14:], uint32(len(path3)))
	copy(val[len(path1)+18:], path3)

	dispatcher.Pak.Value = val

	res, err := syncUnaryCall(
		OpsCrud{
			ExtFramesEnabled:      true,
			CollectionsEnabled:    true,
			DurabilityEnabled:     true,
			PreserveExpiryEnabled: true,
		},
		OpsCrud.LookupIn,
		dispatcher,
		&LookupInRequest{
			Key:       []byte(uuid.NewString()[:6]),
			VbucketID: 1,
			Ops: []LookupInOp{
				{
					Op:   LookupInOpTypeGet,
					Path: []byte("key"),
				},
				{
					Op:   LookupInOpTypeGet,
					Path: []byte("key2"),
				},
				{
					Op:   LookupInOpTypeGet,
					Path: []byte("key3"),
				},
			},
		},
	)
	require.NoError(t, err)

	require.Len(t, res.Ops, 3)

	assert.Equal(t, path1, res.Ops[0].Value)
	assert.ErrorIs(t, res.Ops[1].Err, ErrSubDocPathNotFound)
	assert.Equal(t, path3, res.Ops[2].Value)
}

func TestOpsCrudMutateInErrorCases(t *testing.T) {
	testutils.SkipIfShortTest(t)

	cli := createTestClient(t)
	initialValue := json.RawMessage(`{"key":"value"}`)

	makeSetSubdocOp := func() MutateInOp {
		return MutateInOp{
			Op:    MutateInOpTypeDictSet,
			Path:  []byte("key"),
			Value: []byte(`"value"`),
		}
	}

	doSetOp := func(tt *testing.T, key []byte) {
		_, err := syncUnaryCall(OpsCrud{
			CollectionsEnabled: true,
			ExtFramesEnabled:   true,
		}, OpsCrud.Set, cli, &SetRequest{
			Key:       key,
			VbucketID: 1,
			Value:     initialValue,
			Datatype:  uint8(0x01),
		})
		require.NoError(tt, err)
	}

	type test struct {
		Name          string
		Request       *MutateInRequest
		ExpectedError error
		IsIndexLevel  bool
		RunFirst      func(*testing.T, []byte)
	}

	tests := []test{
		{
			Name: "DocNotFound",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
			},
			ExpectedError: ErrDocNotFound,
		},
		{
			Name: "DocExists",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrDocExists,
			RunFirst:      doSetOp,
		},
		{
			Name: "CasMismatch",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
				Cas: 123455,
			},
			ExpectedError: ErrCasMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "InvalidCombo",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
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
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrSubDocInvalidCombo,
		},
		{
			Name: "InvalidXattrOrder",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key"),
						Value: []byte("value"),
						Flags: SubdocOpFlagXattrPath,
					},
				},
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrSubDocInvalidXattrOrder,
		},
		{
			Name: "InvalidXattrKeyCombo",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key"),
						Value: []byte(`"value"`),
						Flags: SubdocOpFlagXattrPath,
					},
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key2"),
						Value: []byte(`"value"`),
						Flags: SubdocOpFlagXattrPath,
					},
					makeSetSubdocOp(),
				},
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrSubDocXattrInvalidKeyCombo,
		},
		{
			Name: "XattrUnknownMacro",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key"),
						Value: []byte("${something}"),
						Flags: SubdocOpFlagXattrPath | SubdocOpFlagExpandMacros,
					},
				},
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrSubDocXattrUnknownMacro,
		},
		{
			Name: "XattrCannotModifyVattr",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("$document"),
						Value: []byte("value"),
						Flags: SubdocOpFlagXattrPath | SubdocOpFlagExpandMacros,
					},
				},
				Flags: SubdocDocFlagAddDoc,
			},
			ExpectedError: ErrSubDocXattrCannotModifyVAttr,
		},
		{
			Name: "CanOnlyReviveDeletedDocuments",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
				Flags: SubdocDocFlagReviveDocument | SubdocDocFlagAccessDeleted,
			},
			ExpectedError: ErrSubDocCanOnlyReviveDeletedDocuments,
			RunFirst:      doSetOp,
		},
		// {
		// Name: "DeletedDocumentCantHaveValue",
		// Request: &MutateInRequest{
		// 	VbucketID: 1,
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
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocDocTooDeep,
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

				_, err = syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     b,
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocNotJSON",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					makeSetSubdocOp(),
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocNotJSON,
			RunFirst: func(tt *testing.T, key []byte) {
				_, err := syncUnaryCall(OpsCrud{
					CollectionsEnabled: true,
					ExtFramesEnabled:   true,
				}, OpsCrud.Set, cli, &SetRequest{
					Key:       key,
					VbucketID: 1,
					Value:     []byte("imnotjson"),
					Datatype:  uint8(0x01),
				})
				require.NoError(tt, err)
			},
		},
		{
			Name: "SubDocPathNotFound",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeReplace,
						Path:  []byte("idontexit"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathNotFound,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathMismatch",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key[0]"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathMismatch,
			RunFirst:      doSetOp,
		},
		{
			Name: "SubDocPathInvalid",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictSet,
						Path:  []byte("key["),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathInvalid,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocPathTooBig",
		// 	Request: &MutateInRequest{
		// 		VbucketID: 1,
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
		// 		VbucketID: 1,
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
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeCounter,
						Path:  []byte("key"),
						Value: []uint8{1},
					},
				},
				Flags: SubdocDocFlagMkDoc,
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocBadDelta,
		},
		{
			Name: "SubDocPathExists",
			Request: &MutateInRequest{
				VbucketID: 1,
				Ops: []MutateInOp{
					{
						Op:    MutateInOpTypeDictAdd,
						Path:  []byte("key"),
						Value: []byte(`"value"`),
					},
				},
			},
			IsIndexLevel:  true,
			ExpectedError: ErrSubDocPathExists,
			RunFirst:      doSetOp,
		},
		// {
		// 	Name: "SubDocValueTooDeep",
		// 	Request: &MutateInRequest{
		// 		VbucketID: 1,
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

			_, err := syncUnaryCall(
				OpsCrud{
					ExtFramesEnabled:      true,
					CollectionsEnabled:    true,
					DurabilityEnabled:     true,
					PreserveExpiryEnabled: true,
				},
				OpsCrud.MutateIn,
				cli,
				req,
			)
			require.ErrorIs(tt, err, test.ExpectedError)

			if test.IsIndexLevel {
				var subDocErr *SubDocError
				if assert.ErrorAs(tt, err, &subDocErr) {
					// Not really sure if this is testing anything due to zero values.
					assert.Equal(tt, 0, subDocErr.OpIndex)
				}
			}
		})
	}
}

type testCrudDispatcher struct {
	Pak *Packet
}

func (t *testCrudDispatcher) Dispatch(packet *Packet, callback DispatchCallback) (PendingOp, error) {
	go func() {
		callback(t.Pak, nil)
	}()

	return pendingOpNoop{}, nil
}

func (t *testCrudDispatcher) LocalAddr() string {
	return "localaddr"
}

func (t *testCrudDispatcher) RemoteAddr() string {
	return "remoteaddr"
}
