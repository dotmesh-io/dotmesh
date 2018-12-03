package main

import (
	"sync"
	"testing"
)

func TestListMetadata(t *testing.T) {
	fsm := &fsMachine{
		snapshotCache:   make(map[string][]*Snapshot),
		snapshotCacheMu: &sync.RWMutex{},
	}

	fsm.SetSnapshots("123", []*Snapshot{
		{
			Id: "1",
			Metadata: map[string]string{
				"meta": "1",
			},
		},
	})

	ls := fsm.ListSnapshots()

	if ls["123"][0].Id != "1" {
		t.Errorf("failed to set/list metadata")
	}
	if ls["123"][0].Metadata["meta"] != "1" {
		t.Errorf("failed to set/list metadata")
	}

	// modifying original
	fsm.snapshotCache["123"][0].Metadata["meta"] = "2"

	// checking our copy again
	if ls["123"][0].Metadata["meta"] != "1" {
		t.Errorf("failed to set/list metadata")
	}
}

func TestGetSnapshots(t *testing.T) {
	fsm := &fsMachine{
		snapshotCache:   make(map[string][]*Snapshot),
		snapshotCacheMu: &sync.RWMutex{},
	}

	fsm.SetSnapshots("123", []*Snapshot{
		{
			Id: "1",
			Metadata: map[string]string{
				"meta": "1",
			},
		},
	})

	snaps := fsm.GetSnapshots("123")

	if snaps[0].Id != "1" {
		t.Errorf("failed to set/list metadata")
	}
	if snaps[0].Metadata["meta"] != "1" {
		t.Errorf("failed to set/list metadata")
	}
}

func TestGetSnapshotsUnknownNode(t *testing.T) {
	fsm := &fsMachine{
		snapshotCache:   make(map[string][]*Snapshot),
		snapshotCacheMu: &sync.RWMutex{},
	}

	fsm.SetSnapshots("123", []*Snapshot{
		{
			Id: "1",
			Metadata: map[string]string{
				"meta": "1",
			},
		},
	})

	snaps := fsm.GetSnapshots("10000")

	if len(snaps) != 0 {
		t.Errorf("didn't expect to get any snaps")
	}
}
