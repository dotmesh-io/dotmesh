package store

import (
	"context"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func TestListMastersIndex(t *testing.T) {

	kvdb, err := NewKVDBFilesystemStore(&KVDBConfig{
		Type: KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fsID := "123456789"

	// adding container too

	fc := &types.FilesystemContainers{
		FilesystemID: fsID,
		NodeID:       "node-1",
	}

	err = kvdb.SetContainers(fc, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}

	fs := &types.FilesystemMaster{
		FilesystemID: fsID,
		NodeID:       "node-1",
	}

	// adding masters

	err = kvdb.SetMaster(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set master: %s", err)
	}
	// modifying it again
	fs.NodeID = "node-2"
	err = kvdb.SetMaster(fs, &SetOptions{Force: true})
	if err != nil {
		t.Errorf("failed to set master: %s", err)
	}

	fs.NodeID = "node-3"
	err = kvdb.SetMaster(fs, &SetOptions{Force: true})
	if err != nil {
		t.Errorf("failed to set master: %s", err)
	}

	masters, err := kvdb.ListMaster()
	if err != nil {
		t.Fatalf("failed to list: %s", err)
	}

	if len(masters) != 1 {
		t.Errorf("expected to find 1 master, got: %d", len(masters))
	} else {
		if masters[0].NodeID != "node-3" {
			t.Errorf("expected 'node-3', got: %s", masters[0].NodeID)
		}
		if masters[0].Meta.ModifiedIndex != 4 {
			t.Errorf("modified index: %d", masters[0].Meta.ModifiedIndex)
		}
	}
}

func TestWatchMasterAfterDeletion(t *testing.T) {

	kvdb, err := NewKVDBFilesystemStore(&KVDBConfig{
		Type: KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fsID := "123456789"
	found := false

	fs := &types.FilesystemMaster{
		FilesystemID: fsID,
		NodeID:       "node-1",
	}

	err = kvdb.SetMaster(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}

	err = kvdb.SetMaster(&types.FilesystemMaster{
		FilesystemID: "123",
		NodeID:       "node-2",
	}, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}

	kvdb.DeleteMaster("123")

	time.Sleep(100 * time.Millisecond)

	masters, _ := kvdb.ListMaster()
	if len(masters) != 1 {
		t.Errorf("only expected to get one master, got: %d", len(masters))
	} else {
		if masters[0].Meta.Action != types.KVCreate {
			t.Errorf("unexpected KV action: %d", masters[0].Meta.Action)
		}
	}

	err = kvdb.WatchMasters(0, func(fs *types.FilesystemMaster) error {
		if fs.FilesystemID == fsID {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Errorf("watch failed: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("deadline exceeded, watcher not triggered")
		default:
			if !found {
				time.Sleep(300 * time.Millisecond)
				continue
			}
			// success
			return
		}
	}
}
