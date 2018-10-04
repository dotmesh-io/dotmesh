package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"

	"golang.org/x/net/context"

	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/registry"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

// attempt to pull some snapshots from the master, based on some hint that it
// might be possible now
func receivingState(f *fsMachine) stateFn {
	f.transitionedTo("receiving", "calculating")
	log.Printf("entering receiving state for %s", f.filesystemId)
	snapRange, err := f.plausibleSnapRange()

	// by judiciously reading from f.innerRequests, we implicitly take a lock on not
	// changing mount state until we finish receiving or an attempt to change
	// mount state results in us being cancelled and finish cancelling

	if err != nil {
		switch err := err.(type) {
		case *ToSnapsUpToDate:
			// this is fine, we're up-to-date
			return backoffStateWithReason(fmt.Sprintf("receivingState: ToSnapsUpToDate %s got %s", f.filesystemId, err))
		case *NoFromSnaps:
			// this is fine, no snaps; can't replicate yet, but will
			return backoffStateWithReason(fmt.Sprintf("receivingState: NoFromSnaps %s got %s", f.filesystemId, err))
		case *ToSnapsAhead:
			log.Printf("receivingState: ToSnapsAhead %s got %s", f.filesystemId, err)
			// erk, slave is ahead of master
			errx := f.recoverFromDivergence(err.latestCommonSnapshot.Id)
			if errx != nil {
				return backoffStateWithReason(fmt.Sprintf("receivingState(%s): Unable to recover from divergence: %+v", f.filesystemId, errx))
			}
			// Go to discovering state, to update the world with our recent ZFS actions.
			return discoveringState
		case *ToSnapsDiverged:
			log.Printf("receivingState: ToSnapsDiverged %s got %s", f.filesystemId, err)
			errx := f.recoverFromDivergence(err.latestCommonSnapshot.Id)
			if errx != nil {
				return backoffStateWithReason(fmt.Sprintf("receivingState(%s): Unable to recover from divergence: %+v", f.filesystemId, errx))
			}
			// Go to discovering state, to update the world with our recent ZFS actions.
			return discoveringState
		case *NoCommonSnapshots:
			// erk, no common snapshots between master and slave
			// TODO: create a new local clone (branch), then delete the current
			// filesystem to enable replication to continue
			return backoffStateWithReason(fmt.Sprintf("receivingState: NoCommonSnapshots %s got %+v", f.filesystemId, err))
		default:
			return backoffStateWithReason(fmt.Sprintf("receivingState: default error handler %s got %s", f.filesystemId, err))
		}
	}

	var fromSnap string
	if snapRange.fromSnap == nil {
		fromSnap = "START"
		// it's possible this is the first snapshot for a clone. check, and if
		// it is, attempt to generate a replication stream from the clone's
		// origin. it might be the case that the clone's origin doesn't exist
		// here, in which case the apply will fail.
		clone, err := f.state.registry.LookupCloneById(f.filesystemId)
		if err != nil {
			switch err := err.(type) {
			case registry.NoSuchClone:
				// Normal case for non-clone filesystems, continue.
			default:
				return backoffStateWithReason(fmt.Sprintf("receivingState: Error trying to lookup clone by id: %+v", err))
			}
		} else {
			// Found a clone, let's base our pull on it
			fromSnap = fmt.Sprintf(
				"%s@%s", clone.Origin.FilesystemId, clone.Origin.SnapshotId,
			)
		}
	} else {
		fromSnap = snapRange.fromSnap.Id
	}

	addresses := f.state.addressesFor(
		f.state.masterFor(f.filesystemId),
	)
	if len(addresses) == 0 {
		return backoffStateWithReason(fmt.Sprintf("receivingState: No known address for current master of %s", f.filesystemId))
	}

	admin, err := f.state.userManager.Get(&user.Query{Ref: "admin"})
	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: Attempting to pull %s, failed to get admin user, error: %s", f.filesystemId, err))
	}

	url, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey)
	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: deduceUrl failed with %+v", err))
	}

	req, err := http.NewRequest(
		"GET",
		fmt.Sprintf(
			// receiving only happens within clusters. push/pull between
			// clusters is all pushPeerState etc.
			"%s/filesystems/%s/%s/%s", url,
			f.filesystemId, fromSnap, snapRange.toSnap.Id,
		),
		nil,
	)
	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: Attempting to pull %s got %+v", f.filesystemId, err))
	}
	req.SetBasicAuth("admin", admin.ApiKey)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: Attempting to pull %s got %+v", f.filesystemId, err))
	}
	log.Printf(
		"Debug: curl -u admin:[pw] %s/filesystems/%s/%s/%s",
		url,
		f.filesystemId, fromSnap, snapRange.toSnap.Id,
	)

	f.transitionedTo("receiving", "starting")
	logZFSCommand(f.filesystemId, fmt.Sprintf("%s recv %s", ZFS, fq(f.filesystemId)))
	cmd := exec.Command(ZFS, "recv", fq(f.filesystemId))
	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	defer pipeWriter.Close()

	cmd.Stdin = pipeReader
	cmd.Stdout = getLogfile("zfs-recv-stdout")
	cmd.Stderr = getLogfile("zfs-recv-stderr")

	finished := make(chan bool)

	go pipe(
		resp.Body, fmt.Sprintf("http response body for %s", f.filesystemId),
		pipeWriter, "stdin of zfs recv",
		finished,
		f.innerRequests,
		// put the event back on the channel in the cancellation case
		func(e *Event, c chan *Event) { c <- e },
		func(bytes int64, t int64) {
			f.transitionedTo("receiving",
				fmt.Sprintf(
					"transferred %.2fMiB in %.2fs (%.2fMiB/s)...",
					// bytes => mebibytes       nanoseconds => seconds
					float64(bytes)/(1024*1024), float64(t)/(1000*1000*1000),
					// mib/sec
					(float64(bytes)/(1024*1024))/(float64(t)/(1000*1000*1000)),
				),
			)
		},
		"decompress",
	)

	log.Printf("[pull] about to start consuming prelude on %v", pipeReader)
	prelude, err := consumePrelude(pipeReader)
	if err != nil {
		_ = <-finished
		return backoffStateWithReason(fmt.Sprintf("receivingState: error consuming prelude: %+v", err))
	}
	log.Printf("[pull] Got prelude %v", prelude)

	err = cmd.Run()
	f.transitionedTo("receiving", "finished zfs recv")
	pipeReader.Close()
	pipeWriter.Close()
	_ = <-finished
	f.transitionedTo("receiving", "finished pipe")

	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: Got error %+v when running zfs recv for %s, check zfs-recv-stderr.log",
			err, f.filesystemId,
		))
	} else {
		log.Printf("Successfully received %s => %s for %s", fromSnap, snapRange.toSnap.Id, f.filesystemId)
	}
	log.Printf("[pull] about to start applying prelude on %v", pipeReader)
	err = applyPrelude(prelude, fq(f.filesystemId))
	if err != nil {
		return backoffStateWithReason(fmt.Sprintf("receivingState: Error applying prelude: %+v", err))
	}
	return discoveringState
}
