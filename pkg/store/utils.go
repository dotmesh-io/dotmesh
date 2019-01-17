package store

import (
	"encoding/json"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"
)

func (s *KVDBFilesystemStore) encode(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

func (s *KVDBFilesystemStore) decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func getMeta(kvp *kvdb.KVPair) *types.KVMeta {

	var action types.KVAction
	switch kvp.Action {
	case kvdb.KVSet:
		action = types.KVSet
	case kvdb.KVCreate:
		action = types.KVCreate
	case kvdb.KVGet:
		action = types.KVGet
	case kvdb.KVDelete:
		action = types.KVDelete
	case kvdb.KVExpire:
		action = types.KVExpire
	case kvdb.KVUknown:
		action = types.KVUknown
	}

	return &types.KVMeta{
		KVDBIndex:     kvp.KVDBIndex,
		CreatedIndex:  kvp.CreatedIndex,
		ModifiedIndex: kvp.ModifiedIndex,
		Action:        action,
	}
}
