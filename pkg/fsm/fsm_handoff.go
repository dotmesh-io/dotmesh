package fsm

import (
	"fmt"
	"log"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"golang.org/x/net/context"
)

// state functions
// invariant: whenever a state function receives on the events channel, it
// should respond with a response event, even in an error case.

func handoffState(f *FsMachine) StateFn {
	f.transitionedTo("handoff", "starting...")
	// I am a master, trying to move this filesystem to a slave.
	// I got put into this state in response to a "move" event on f.requests,
	// so it's my responsibility to put something onto f.responses, because
	// there'll be someone out there listening for my response...
	// I assume that previous states stopped any containers that were running
	// on this filesystem, so the filesystem is quiescent.
	// TODO stop any containers being able to get started here.
	target := (*f.handoffRequest.Args)["target"].(string)
	log.Printf("Found target node %s", target)

	// subscribe for snapshot updates before we start sending, in case of races...
	newSnapsChan := make(chan interface{})
	f.newSnapsOnServers.Subscribe(target, newSnapsChan)
	defer f.newSnapsOnServers.Unsubscribe(target, newSnapsChan)

	if target == f.state.NodeID() {
		errString := fmt.Sprintf("Error trying to handoff because myNodeId: %s and target: %s are the same", f.state.NodeID(), target)
		log.Println(errString)
		f.innerResponses <- &types.Event{
			Name: "error-handoff-source-target-are-equal",
			Args: &types.EventArgs{"err": errString},
		}
		return backoffState
	}

	// unmount the filesystem immediately, so that the filesystem doesn't get
	// dirtied by being unmounted
	event, _ := f.unmount()
	if event.Name != "unmounted" {
		log.Printf("unexpected response to unmount attempt: %s", event)
		f.innerResponses <- event
		return backoffState
	}

	// XXX if we error out of handoffState, we'll end up in an infinite loop if
	// we don't re-mount the filesystem. see comment in backoffState for
	// possible fix.

	// take a snapshot and wait for it to arrive on the target
	response, _ := f.snapshot(&types.Event{
		Name: "snapshot",
		Args: &types.EventArgs{"metadata": types.Metadata{
			"type":   "migration",
			"author": "system",
			"message": fmt.Sprintf(
				"Automatic snapshot during migration from %s to %s.",
				f.state.NodeID(), target,
			)},
		},
	})
	f.transitionedTo("handoff", fmt.Sprintf("got snapshot response %s", response))
	if response.Name != "snapshotted" {
		// error - bail
		f.innerResponses <- response
		return backoffState
	}
	slaveUpToDate := false

waitingForSlaveSnapshot:
	for !slaveUpToDate {
		// ok, so snapshot succeeded. wait for it to be replicated to the
		// target node (it should be, naturally because currently we replicate
		// everything everywhere)
		f.transitionedTo("handoff", fmt.Sprintf("calling snapshotsFor %s", target))
		slaveSnapshots, err := f.state.SnapshotsFor(target, f.filesystemId)
		f.transitionedTo(
			"handoff",
			fmt.Sprintf("done calling snapshotsFor %s: %s", target, err),
		)
		if err != nil {
			// Let's assume that no record of snapshots on a node means no
			// filesystem there. If we're wrong and there /is/ a filesystem
			// there with no snapshots, we won't be able to receive into it.
			// But this shouldn't happen because you can only create a
			// filesystem if you can write atomically to etcd, claiming its
			// name for yourself.
			log.Printf(
				"Unable to find target snaps for %s on %s, assuming there are none and proceeding...",
				f.filesystemId, target,
			)
		}
		f.transitionedTo(
			"handoff",
			fmt.Sprintf("finding own snaps for move to %s", target),
		)

		// information about our new snapshot probably hasn't roundtripped
		// through etcd yet, so use our definitive knowledge about our local
		// state...

		snaps := f.ListLocalSnapshots()

		f.transitionedTo(
			"handoff",
			fmt.Sprintf("done finding own snaps for move to %s", target),
		)

		apply, err := canApply(snaps, pointers(slaveSnapshots))
		f.transitionedTo(
			"handoff",
			fmt.Sprintf("canApply returned %+v, %v", apply, err),
		)
		if err != nil {
			switch err.(type) {
			case *ToSnapsUpToDate:
				log.Printf("Found ToSnapsUpToDate, setting slaveUpToDate for %s", f.filesystemId)
				slaveUpToDate = true
				break waitingForSlaveSnapshot
			}
		} else {
			err = fmt.Errorf(
				"ff update of %s for %s to %s was possible, can't move yet, retrying...",
				f.filesystemId, f.state.NodeID(), target,
			)
		}
		if !slaveUpToDate {
			log.Printf(
				"Not proceeding with migration yet for %s from %s to %s because %s, waiting for new snaps...",
				f.filesystemId, f.state.NodeID(), target, err,
			)
		}

		// TODO timeout, or liveness check on replication
		log.Printf("About to read from newSnapsChan(%s) we created earlier", target)

		// say no to everything right now, but don't clog up requests
		gotSnaps := false
		for !gotSnaps {
			select {
			case e := <-f.innerRequests:
				// What if a deletion message comes in here?

				// In that case, the deletion will happen later, when we
				// go into discovery again and perform the check for the
				// filesystem being deleted.
				log.Printf("rejecting all %s", e)
				f.innerResponses <- types.NewEvent("busy-handoff")
			case _ = <-newSnapsChan:
				// TODO check that the latest snap is the one we expected
				gotSnaps = true
				log.Printf("Got new snaps of %s on %s", f.filesystemId, target)
				// carry on
			}
		}
	}
	// cool, fs is quiesced and latest snap is on target. switch!

	_, err := f.etcdClient.Set(
		context.Background(),
		fmt.Sprintf(
			"%s/filesystems/masters/%s", types.EtcdPrefix, f.filesystemId,
		),
		target,
		// only modify current master if I am indeed still the master
		&client.SetOptions{PrevValue: f.state.NodeID()},
	)
	if err != nil {
		f.innerResponses <- &types.Event{
			Name: "failed-to-set-master-in-etcd",
			Args: &types.EventArgs{
				"err":    err,
				"target": f.filesystemId,
				"node":   f.state.NodeID(),
			},
		}
		return backoffState
	}
	f.innerResponses <- &types.Event{Name: "moved"}
	return inactiveState
}
