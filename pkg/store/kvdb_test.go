package store

import (
	"context"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func TestWatchContainersA(t *testing.T) {

	kvdb, err := NewKVDBFilesystemStore(&KVDBConfig{
		Type: KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fsID := "123456789"
	found := false

	err = kvdb.WatchContainers(0, func(fs *types.FilesystemContainers) error {
		if fs.FilesystemID == fsID {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Errorf("watch failed: %s", err)
	}

	fs := &types.FilesystemContainers{
		FilesystemID: fsID,
		NodeID:       "node-1",
	}

	err = kvdb.SetContainers(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
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

// Watching directory after multiple changes
func TestWatchContainersLater(t *testing.T) {

	kvdb, err := NewKVDBFilesystemStore(&KVDBConfig{
		Type: KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fsID := "123456789"
	var modifiedIdx uint64
	found := false

	fs := &types.FilesystemContainers{
		FilesystemID: fsID,
		NodeID:       "node-1",
	}

	err = kvdb.SetContainers(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}
	// modifying it again
	fs.NodeID = "node-2"
	err = kvdb.SetContainers(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}

	fs.NodeID = "node-3"
	err = kvdb.SetContainers(fs, &SetOptions{})
	if err != nil {
		t.Errorf("failed to set container: %s", err)
	}

	time.Sleep(1 * time.Second)

	err = kvdb.WatchContainers(2, func(fs *types.FilesystemContainers) error {
		t.Logf("event %d, modified idx: %d, fsID: %s", fs.Meta.Action, fs.Meta.ModifiedIndex, fs.FilesystemID)
		modifiedIdx = fs.Meta.ModifiedIndex
		if fs.FilesystemID == fsID {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Errorf("watch failed: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

			if modifiedIdx != 3 {
				t.Errorf("expected modified index to be 3, got: %d", modifiedIdx)
			}

			return
		}
	}
}
