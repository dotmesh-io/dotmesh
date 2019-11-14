package zfs

import (
	"fmt"
	"testing"
	"time"
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

func TestZFSDiffCaching(t *testing.T) {
	// TODO presupposes pool named test was already created...
	z, err := NewZFS("zfs", "zpool", "test", "test")
	if err != nil || z == nil {
		t.Fatalf("Error creating pool: %s", err)
	}
	z.FindFilesystemIdsOnSystem()
	output, err := z.Create("mytestfs")
	if err != nil {
		t.Fatalf("Error creating fs: %s\n%s", err, output)
	}
	defer z.DeleteFilesystemInZFS("mytestfs")
	output, err = z.Snapshot("mytestfs", "myfirstsnapshot", []string{})
	if err != nil {
		t.Fatalf("Error snapshotting: %s\n%s", err, output)
	}
	fmt.Printf("DIFF TIME!\n")
	_, err = z.Diff("mytestfs")
	if err != nil {
		t.Fatalf("Error diffing: %s\n", err)
	}
	_, err = z.Diff("mytestfs")
	if err != nil {
		t.Fatalf("Error diffing second time: %s\n", err)
	}
}
