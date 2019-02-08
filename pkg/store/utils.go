package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"
)

func (s *KVDBFilesystemStore) encode(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

func (s *KVDBFilesystemStore) decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (s *KVServerStore) encode(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

func (s *KVServerStore) decode(data []byte, v interface{}) error {
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

func ActionString(a kvdb.KVAction) string {
	switch a {
	case kvdb.KVSet:
		return "KVSet"
	case kvdb.KVCreate:
		return "KVCreate"
	case kvdb.KVGet:
		return "KVGet"
	case kvdb.KVDelete:
		return "KVDelete"
	case kvdb.KVExpire:
		return "KVExpire"
	case kvdb.KVUknown:
		return "KVUknown"
	}
	return fmt.Sprintf("unknown: %d", a)
}

func extractID(key string) (string, error) {
	pieces := strings.Split(key, "/")

	if len(pieces) < 1 {
		return "", fmt.Errorf("failed to extract ID, check your key: %s", key)
	}

	return pieces[len(pieces)-1], nil
}

func extractIDs(key string) (string, string, error) {
	pieces := strings.Split(key, "/")

	if len(pieces) < 1 {
		return "", "", fmt.Errorf("failed to extract ID, check your key: %s", key)
	}

	return pieces[len(pieces)-2], pieces[len(pieces)-1], nil
}
