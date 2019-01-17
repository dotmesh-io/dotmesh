package store

import (
	"context"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func TestWatchContainers(t *testing.T) {

	kvdb, err := NewKVDBFilesystemStore(&KVDBConfig{
		Type: KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}

	fsID := "123456789"
	found := false

	err = kvdb.WatchContainers(func(fs *types.FilesystemContainers) error {
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
