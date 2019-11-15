package zfs

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

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

func createPoolAndFilesystem(t *testing.T) (z ZFS, fsName, fsMountPath string, cleanup cleanupFunc) {
	// Delete any pre-existing pool-id.
	os.Remove("/dotmesh-pool-id")

	// Create the pool
	poolName := "fs" + uuid.New().String()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Can't get working directory!? %s", err)
	}
	poolPath := wd + "/" + poolName + ".zpool"
	mustRun(t, "truncate", "-s", "100M", poolPath)
	mustRun(t, "zpool", "create", poolName, poolPath)
	fsName = "mytestfs"
	fsMountPath = "/" + poolName + "/dmfs/" + fsName + "/"

	// Create the zfs object
	z, err = NewZFS("zfs", "zpool", poolName, poolName)
	if err != nil || z == nil {
		t.Fatalf("Error creating pool: %s", err)
	}

	// Create the dmfs filesystem structure
	z.FindFilesystemIdsOnSystem()

	// Create the filesystem
	output, err := z.Create("mytestfs")
	if err != nil {
		t.Fatalf("Error creating fs: %s\n%s", err, output)
	}

	cleanup = func() {
		// If env variable is set, don't destroy the pool at end of test, for easier
		// debugging.
		if os.Getenv("ZFS_TEST_DEBUG") == "" {
			mustRun(t, "zpool", "destroy", poolName)
			os.Remove(poolPath)
		}
	}
	return z, fsName, fsMountPath, cleanup
}

func TestZFSDiffCaching(t *testing.T) {
	z, fsName, _, cleanup := createPoolAndFilesystem(t)
	defer cleanup()

	output, err := z.Snapshot(fsName, "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}
	fmt.Printf("DIFF TIME!\n")
	_, err = z.Diff(fsName)
	if err != nil {
		t.Fatalf("Error diffing: %s\n", err)
	}
	_, err = z.Diff(fsName)
	if err != nil {
		t.Fatalf("Error diffing second time: %s\n", err)
	}
	_, err = z.Diff(fsName)
	if err != nil {
		t.Fatalf("Error diffing second time: %s\n", err)
	}
}
