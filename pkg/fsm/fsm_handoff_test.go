package fsm

import (
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func TestUpdateMasterA(t *testing.T) {

	kvdb, err := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fm := &types.FilesystemMaster{
		NodeID:       "1",
		FilesystemID: "fs-id",
	}

	err = kvdb.SetMaster(fm, &store.SetOptions{})

	if err != nil {
		t.Errorf("failed to create master: %s", err)
	}

	err = updateTargetMasterIfMatches(kvdb, "fs-id", "2", "1")
	if err != nil {
		t.Errorf("failed to update master: %s", err)
	}

	updated, err := kvdb.GetMaster("fs-id")
	if err != nil {
		t.Fatalf("failed to get fm: %s", err)
	}
	if updated.NodeID != "2" {
		t.Errorf("expected to find node '2', got: '%s'", updated.NodeID)
	}
}

func TestUpdateMasterB(t *testing.T) {

	kvdb, err := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fm := &types.FilesystemMaster{
		NodeID:       "3",
		FilesystemID: "fs-id",
	}

	err = kvdb.SetMaster(fm, &store.SetOptions{})

	if err != nil {
		t.Errorf("failed to create master: %s", err)
	}

	err = updateTargetMasterIfMatches(kvdb, "fs-id", "2", "1")
	if err == nil {
		t.Errorf("expected to get an error when updating the node")
	}

	updated, err := kvdb.GetMaster("fs-id")
	if err != nil {
		t.Fatalf("failed to get fm: %s", err)
	}
	if updated.NodeID == "2" {
		t.Errorf("didn't expect to find node '2', got: '%s'", updated.NodeID)
	}
}
