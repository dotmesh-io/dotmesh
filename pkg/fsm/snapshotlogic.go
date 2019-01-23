package fsm

// functions that reason about sequences of snapshots

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// given two slices of snapshots (representing the snapshots of supposedly the
// same filesystem on two nodes), calculate whether it's possible to apply a
// subsequence of fromSnaps (source snapshots) to toSnaps (destination
// snapshots), and if so, return the maximal range of snapshots in fromSnaps
// which can be applied to toSnaps to bring it up-to-date.
//
// in "git" terminology, this is similar to whether a "fast forward update" can
// be applied:
//
// a non-error response from this function doesn't guarantee that a replication
// stream generated from fromSnaps can be applied to the filesystem represented
// by toSnaps: it may be the case that unsnapshotted changes exist on the
// toSnaps side, a condition that can be diagnosed by trying to send and then
// observing the error from the filesystem.
//
// if no such subsequence can be found, return one of the following errors:
//
// NoFromSnaps: fromSnaps was empty, can't apply anything!
//
// NoCommonSnapshots: there are no common snapshots between the two sequences.
//     this might imply that they were not from the same filesystem to begin
//     with, or that snapshots were deleted on both sides such that they are
//     now irreconcileable (e.g. fromSnaps has A, C, E, toSnaps has B, D). In
//     either case, the only way to proceed with synchronization between the
//     two is to destroy one of them (or rename it) and start replication again
//     from scratch.
//
// ToSnapsDiverged: there are snapshots after the latest common snapshot in the
//     toSnaps slice. These snapshots need to be removed (or the filesystem
//     cloned to save them, and the original filesystem rolled back) in order
//     to be able to apply further any of the updates in. In this case the
//     error will include the latest common snapshot as 'latestCommonSnapshot'.
//
// ToSnapsAhead: there are snapshots in toSnaps which come after the latest
//     snapshot in fromSnaps. this can happen if the source filesystem was an
//     out-of-date slave which got force-promoted to a master, and then the old
//     master comes back and tries to pull from it.
//
// ToSnapsUpToDate: there are no new snapshots in fromSnaps to apply to
//     toSnaps: toSnaps is already up-to-date.

func canApply(fromSnaps []*types.Snapshot, toSnaps []*types.Snapshot) (*snapshotRange, error) {
	// fromSnaps and toSnaps are in-order.
	//
	// case: fromSnaps empty
	// =====================
	if len(fromSnaps) == 0 {
		return nil, &NoFromSnaps{}
	}
	// case: toSnaps empty
	// ===================
	//
	// fromSnaps:  A--B--C--D
	// toSnaps:
	//
	// result:     nil------D, nil

	if len(toSnaps) == 0 {
		return &snapshotRange{
			fromSnap: nil,
			toSnap:   fromSnaps[len(fromSnaps)-1],
		}, nil
	}

	// build a map (its keys are like a set) of toSnaps ids, so we can quickly
	// check whether toSnap ids are in fromSnaps as we iterate over it.

	toSnapKeys := map[string]bool{}
	for _, snap := range toSnaps {
		toSnapKeys[snap.Id] = true
	}

	var latestCommon *types.Snapshot
	// find latest common snapshot
	for i := len(fromSnaps) - 1; i >= 0; i-- {
		maybeCommon := fromSnaps[i].Id
		if _, ok := toSnapKeys[maybeCommon]; ok {
			latestCommon = fromSnaps[i]
			break
		}
	}
	// case: completely diverged
	// =========================
	//
	// fromSnaps:  A
	// toSnaps:    B

	if latestCommon == nil {
		return nil, &NoCommonSnapshots{
			FromSnaps: fromSnaps,
			ToSnaps:   toSnaps,
		}
	}

	if latestCommon.Id == toSnaps[len(toSnaps)-1].Id {
		// case toSnaps up-to-date
		// =======================
		//
		// fromSnaps:  A--B
		// toSnaps:    A--B

		if latestCommon.Id == fromSnaps[len(fromSnaps)-1].Id {
			return nil, &ToSnapsUpToDate{}
		}

		// case: toSnaps behind (ff-update)
		// ================================
		//
		// fromSnaps:  A--B--C--D
		// toSnaps:    A--B
		//
		// result:        B-----D, nil

		return &snapshotRange{
			fromSnap: latestCommon,
			toSnap:   fromSnaps[len(fromSnaps)-1],
		}, nil
	}

	if latestCommon.Id == fromSnaps[len(fromSnaps)-1].Id {
		// case: toSnaps ahead
		// ===================
		//
		// fromSnaps:  A--B
		// toSnaps:    A--B--C--D

		return nil, &ToSnapsAhead{*latestCommon}
	}

	// case: toSnaps diverged
	// ======================
	//
	// fromSnaps:  A--B--C--D
	// toSnaps:    A--B--E--F

	return nil, &ToSnapsDiverged{*latestCommon}

}

type snapshotRange struct {
	fromSnap *types.Snapshot
	toSnap   *types.Snapshot
}

type NoFromSnaps struct{}

func (e *NoFromSnaps) Error() string {
	return "no snapshots in source"
}

type NoCommonSnapshots struct {
	FromSnaps []*types.Snapshot
	ToSnaps   []*types.Snapshot
}

func (e *NoCommonSnapshots) Error() string {
	return fmt.Sprintf("no common snapshots from %+v to %+v", e.FromSnaps, e.ToSnaps)
}

type ToSnapsDiverged struct {
	latestCommonSnapshot types.Snapshot
}

func (e *ToSnapsDiverged) Error() string {
	return fmt.Sprintf(
		"toSnaps diverged (latest common snapshot=%+v)",
		e.latestCommonSnapshot,
	)
}

type ToSnapsAhead struct {
	latestCommonSnapshot types.Snapshot
}

func (e *ToSnapsAhead) Error() string {
	return "toSnaps is ahead"
}

type ToSnapsUpToDate struct{}

func (e *ToSnapsUpToDate) Error() string {
	return "toSnaps is up-to-date"
}

func restrictSnapshots(localSnaps []*types.Snapshot, toSnapshotId string) ([]*types.Snapshot, error) {
	if toSnapshotId != "" {
		newLocalSnaps := []*types.Snapshot{}
		for _, s := range localSnaps {
			newLocalSnaps = append(newLocalSnaps, s)
			if s.Id == toSnapshotId {
				return newLocalSnaps, nil
			}
		}
		return newLocalSnaps, fmt.Errorf("Unable to find %s in %+v", toSnapshotId, localSnaps)
	}
	return localSnaps, nil
}
