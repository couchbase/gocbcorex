package core

import (
	"errors"

	"github.com/couchbase/stellar-nebula/core/memdx"
)

type memdOps struct {
	CollectionsEnabled bool
}

func (o memdOps) EncodeCollectionIDToKey(cli KvClient, collectionID uint32, key []byte, buf []byte) ([]byte, error) {
	if !o.CollectionsEnabled {
		if collectionID != 0 {
			return nil, errors.New("collections is not supported")
		}

		return key, nil
	}

	// TODO(brett19): Do LEB Encoding here...
	genKey := append([]byte{ /*LEB BYTES HERE*/ }, key...)
	return genKey, nil
}

func (o memdOps) SASLListMechs(cli KvClient, cb func([]string, error)) {
	cli.Dispatch(&memdx.Packet{
		OpCode: memdx.OpCodeSASLListMechs,
	}, func(resp *memdx.Packet, err error) bool {
		// TODO(brett19): Implement fetching the mechanmisms list
		cb(nil, nil)

		return false
	})
}

func (o memdOps) SASLAuth(cli KvClient, cb func([]byte, error)) {

}

func (o memdOps) SASLContinue(cli KvClient, cb func([]byte, error)) {

}

func (o memdOps) Get(cli KvClient, key []byte, collectionID uint32, cb func([]byte, error)) {
	genKey, err := o.EncodeCollectionIDToKey(cli, collectionID, key, nil)
	if err != nil {
		cb(nil, err)
		return
	}

	// Perform the dispatch here...
	genKey = genKey

}
