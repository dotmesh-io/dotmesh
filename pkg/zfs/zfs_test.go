package zfs

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"
)

var out = `CREATION
Tue Oct 15 11:01 2019`

func TestParseCreationOutput(t *testing.T) {
	t1, err := parseSnapshotCreationTime(out)
	if err != nil {
		t.Fatalf("failed to parse: %s", err)
	}

	if t1.Year() != 2019 {
		t.Errorf("expected 2019, got: %d", t1.Year())
	}
	if t1.Month() != time.October {
		t.Errorf("expected October, got: %s", t1.Month())
	}
	if t1.Day() != 15 {
		t.Errorf("expected 15 day, got: %d", t1.Day())
	}
	if t1.Hour() != 11 {
		t.Errorf("expected 11 hour, got: %d", t1.Hour())
	}
	if t1.Minute() != 1 {
		t.Errorf("expected 1 Minute, got: %d", t1.Minute())
	}
}

type cleanupFunc func()

// Run command with stdout and stderr printed.
func verboseRun(program string, args ...string) error {
	cmd := exec.Command(program, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Fail test if commands fails.
func mustRun(t *testing.T, program string, args ...string) {
	err := verboseRun(program, args...)
	if err != nil {
		t.Fatalf("Failed to run %s %#v: %s", program, args, err)
	}
}

func createPoolAndFilesystem(t *testing.T) (z ZFS, fsName, defaultDotPath string, cleanup cleanupFunc) {
	err := exec.Command("zpool", "events").Run()
	if err != nil {
		t.Skipf("Failed to run zpool, you typically need to run as root to run this test: %s", err)
	}

	os.Setenv("MOUNT_PREFIX", "/tmp/zfstest")

	// Delete any pre-existing pool-id.
	os.Remove("/dotmesh-pool-id")

	// Create the pool
	poolName := "fs" + uuid.New().String()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Can't get working directory!? %s", err)
	}
	poolPath := filepath.Join(wd, poolName+".zpool")
	mustRun(t, "truncate", "-s", "100M", poolPath)
	mustRun(t, "zpool", "create", poolName, poolPath)
	fsName = "fs" + uuid.New().String()
	fsMountPath := filepath.Join("/tmp/zfstest", poolName, "dmfs", fsName)
	defaultDotPath = filepath.Join(fsMountPath, "__default__")

	// Create the zfs object
	z, err = NewZFS("zfs", "zpool", poolName, "mount.zfs")
	if err != nil || z == nil {
		t.Fatalf("Error creating pool: %s", err)
	}

	// Create the dmfs filesystem structure
	z.FindFilesystemIdsOnSystem()

	// Create the filesystem
	output, err := z.Create(fsName)
	if err != nil {
		t.Fatalf("Error creating fs: %s\n%s", err, output)
	}

	// Mount the filesystem
	output, err = z.Mount(fsName, "", "", fsMountPath)
	if err != nil {
		t.Fatalf("Error mounting fs: %s\n%s", err, output)
	}

	// Create the default dot (__default__):
	err = os.MkdirAll(defaultDotPath, 0775)
	if err != nil {
		t.Fatalf("Failed to create __default__: %s", err)
	}

	cleanup = func() {
		// If env variable is set, don't destroy the pool at end of test, for easier
		// debugging.
		if os.Getenv("ZFS_TEST_DEBUG") == "" {
			mustRun(t, "zpool", "destroy", poolName)
			os.Remove(poolPath)
		}
	}
	return z, fsName, defaultDotPath, cleanup
}

// No changes since last snapshot, diff is empty
func TestZFSDiffNoChanges(t *testing.T) {
	z, fsName, _, cleanup := createPoolAndFilesystem(t)
	defer cleanup()
	output, err := z.Snapshot(fsName, "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}
	expectChangesFromDiff(t, z, fsName)
}

// Assert a particular ZFSFileDiff is expected from zfs.Diff
func expectChangesFromDiff(t *testing.T, z ZFS, fsName string, expectedChanges ...types.ZFSFileDiff) {
	if expectedChanges == nil {
		expectedChanges = []types.ZFSFileDiff{}
	}
	check := func(expectFromCache bool) {
		currentLogLevel := logrus.StandardLogger().GetLevel()
		logrus.SetLevel(logrus.DebugLevel)
		defer logrus.SetLevel(currentLogLevel)
		hook := logtest.NewGlobal()
		changes, err := z.Diff(fsName)
		if err != nil {
			t.Fatalf("Error diffing: %s\n", err)
		}
		if len(changes) != len(expectedChanges) {
			t.Fatalf("Wrong # changes recorded: %#v", changes)
		}
		if !reflect.DeepEqual(changes, expectedChanges) {
			t.Fatalf("Wrong changes: %#v != %#v\n", changes, expectedChanges)
		}
		for _, entry := range hook.Entries {
			if entry.Data["diff_used_cache"] == nil {
				continue
			}
			if entry.Data["diff_used_cache"] == expectFromCache {
				// What we expected, great!
				return
			} else {
				t.Errorf("Cache usage was not as expected: expected %#v != actual %#v", expectFromCache, entry.Data["diff_used_cache"])
			}
		}
		t.Errorf("Couldn't find entry in logs about caching mode?!")
	}

	// The first time we don't expect the cache to be used:
	check(false)

	// If we do diff a second time, we should:
	// 1. Get the same result.
	// 2. Use the fast path using cached results.
	check(true)
}

// File added since last snapshot, diff has it:
func TestZFSDiffFileAdded(t *testing.T) {
	z, fsName, fsPath, cleanup := createPoolAndFilesystem(t)
	defer cleanup()
	output, err := z.Snapshot(fsName, "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}

	filePath := filepath.Join(fsPath, "myfile.txt")
	err = ioutil.WriteFile(filePath, []byte("woo"), 0644)
	if err != nil {
		t.Fatalf("Error creating file: %s", err)
	}
	expectChangesFromDiff(t, z, fsName, types.ZFSFileDiff{Change: types.FileChangeAdded, Filename: "myfile.txt"})
}

// File deleted since last snapshot, diff has it:
func TestZFSDiffFileDeleted(t *testing.T) {
	z, fsName, fsPath, cleanup := createPoolAndFilesystem(t)
	defer cleanup()

	filePath := filepath.Join(fsPath, "myfile.txt")
	err := ioutil.WriteFile(filePath, []byte("woo"), 0644)
	if err != nil {
		t.Fatalf("Error creating file: %s", err)
	}

	output, err := z.Snapshot(fsName, "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}
	err = os.Remove(filePath)
	if err != nil {
		t.Fatalf("Error removing: %s\n", err)
	}

	expectChangesFromDiff(t, z, fsName, types.ZFSFileDiff{Change: types.FileChangeRemoved, Filename: "myfile.txt"})
}

// File modified since last snapshot, diff has it:
func TestZFSDiffFileModified(t *testing.T) {
	z, fsName, fsPath, cleanup := createPoolAndFilesystem(t)
	defer cleanup()

	filePath := filepath.Join(fsPath, "myfile.txt")
	err := ioutil.WriteFile(filePath, []byte("woo"), 0644)
	if err != nil {
		t.Fatalf("Error creating file: %s", err)
	}

	output, err := z.Snapshot(fsName, "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}
	err = ioutil.WriteFile(filePath, []byte("abc"), 0644)
	if err != nil {
		t.Fatalf("Error changing file: %s", err)
	}

	expectChangesFromDiff(t, z, fsName, types.ZFSFileDiff{Change: types.FileChangeModified, Filename: "myfile.txt"})
}
