package fsm

import (
	"fmt"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

func pushPeerState(f *FsMachine) StateFn {
	// we are responsible for putting something back onto the channel
	f.transitionedTo("pushPeerState", "running")

	newSnapsOnMaster := make(chan interface{})
	receiveProgress := make(chan interface{})
	log.Infof("[pushPeerState] subscribing to newSnapsOnMaster for %s", f.filesystemId)

	f.localReceiveProgress.Subscribe(f.filesystemId, receiveProgress)
	defer f.localReceiveProgress.Unsubscribe(f.filesystemId, receiveProgress)

	f.newSnapsOnMaster.Subscribe(f.filesystemId, newSnapsOnMaster)
	defer f.newSnapsOnMaster.Unsubscribe(f.filesystemId, newSnapsOnMaster)

	// this is a write state. refuse to act if containers are running

	// refuse to be pushed into if we have any containers running
	// TODO stop any containers being started, somehow.
	containers, err := f.containersRunning()
	if err != nil {
		log.Printf(
			"Can't receive push for filesystem while we can't list whether containers are using it",
		)
		f.innerResponses <- &types.Event{
			Name: "error-listing-containers-during-push-receive",
			Args: &types.EventArgs{"err": fmt.Sprintf("%v", err)},
		}
		return backoffState
	}
	if len(containers) > 0 {
		log.Printf("Can't receive push for filesystem while containers are using it")
		f.innerResponses <- &types.Event{
			Name: "cannot-receive-push-while-containers-running",
			Args: &types.EventArgs{"containers": containers},
		}
		return backoffState
	}

	// wait for the desired snapshot to exist here. this means that completing
	// a receive operation must prompt us into loading, but without forgetting
	// that we were in here, so some kind of inline-loading.

	// what is the desired snapshot?
	targetSnapshot := f.lastTransferRequest.TargetCommit

	// XXX are we allowed to transitively receive into other filesystems,
	// without synchronizing with their state machines?

	// first check whether we already have the snapshot. if so, early
	// exit?
	ss, err := f.state.SnapshotsFor(f.state.NodeID(), f.filesystemId)
	for _, s := range ss {
		if s.Id == targetSnapshot {
			f.innerResponses <- &types.Event{
				Name: "receiving-push-complete",
				Args: &types.EventArgs{},
			}
			log.Printf(
				"[pushPeerState:%s] snaps-already-exist case, "+
					"returning activeState on snap %s",
				f.filesystemId, targetSnapshot,
			)
			return activeState
		}
	}

	timeoutTimer := time.NewTimer(600 * time.Second)
	finished := make(chan bool)

	// reset timer when progress is made
	reset := func() {
		// copied from https://golang.org/pkg/time/#Timer.Reset
		if !timeoutTimer.Stop() {
			<-timeoutTimer.C
		}
		timeoutTimer.Reset(600 * time.Second)
	}

	go func() {
		for {
			select {
			case <-receiveProgress:
				//log.Printf(
				//	"[pushPeerState] resetting timer because some progress was made (%d bytes)", b,
				//)
				reset()
			case <-finished:
				return
			}
		}
	}()

	// allow timer-resetter goroutine to exit as soon as we exit this function
	defer func() {
		go func() {
			finished <- true
		}()
	}()

	// Here we are about to block, so confirm we are ready at this
	// point or the caller won't start to push and unblock us
	log.Infof("[pushPeerState:%s] clearing peer to send", f.filesystemId)
	f.innerResponses <- &types.Event{
		Name: "awaiting-transfer",
		Args: &types.EventArgs{},
	}

	log.Infof("[pushPeerState:%s] blocking for ZFSReceiver to tell us to proceed via pushCompleted", f.filesystemId)

	select {
	case <-timeoutTimer.C:
		log.Warnf(
			"[pushPeerState:%s] Timed out waiting for pushCompleted",
			f.filesystemId,
		)
		return backoffState
	case success := <-f.pushCompleted:
		// onwards!
		if !success {
			log.Printf(
				"[pushPeerState:%s] ZFS receive failed.",
				f.filesystemId,
			)
			return backoffState
		}
	}
	log.Infof("[pushPeerState:%s] ZFS receive succeeded.", f.filesystemId)

	// inline load, async because discover() blocks on publishing to
	// newSnapsOnMaster chan, which we're subscribed to and so have to read
	// from concurrently with discover() to avoid deadlock.
	go func() {
		err = f.discover()
		log.Infof("[pushPeerState:%s] done inline load", f.filesystemId)
		if err != nil {
			// XXX how to propogate the error to the initiator? should their
			// retry include sending a new peer-transfer message every time?
			log.Errorf("[pushPeerState:%s] error during inline load: %s", f.filesystemId, err)
		}
	}()

	// give ourselves another 60 seconds while loading
	// log.Printf("[pushPeerState] resetting timer because we're waiting for loading")
	reset()

	for {
		// Loops as notifications of the new snapshots arrive
		// log.Printf("[pushPeerState] about to read from newSnapsOnMaster")
		select {
		case <-timeoutTimer.C:
			log.Warnf("[pushPeerState:%s] timed out waiting for newSnapsOnMaster", f.filesystemId)
			return backoffState
		// check that the snapshot is the one we're expecting
		case s := <-newSnapsOnMaster:
			sn := s.(*types.Snapshot)
			log.Infof("[pushPeerState:%s]  got snapshot %+v while waiting for one to arrive", f.filesystemId, sn)
			if sn.Id == targetSnapshot {

				f.snapshotsLock.Lock()
				mounted := f.filesystem.Mounted
				f.snapshotsLock.Unlock()

				if mounted {
					log.Infof(
						"[pushPeerState:%s] mounted case, returning activeState on snap %s",
						f.filesystemId, sn.Id,
					)
					return activeState
				} else {
					// XXX does mounting alone dirty the filesystem, stopping
					// receiving further pushes?
					responseEvent, nextState := f.mount()
					if responseEvent.Name == "mounted" {
						log.Infof(
							"[pushPeerState:%s] unmounted case, returning nextState %+v on snap %s",
							f.filesystemId, nextState, sn.Id,
						)
						return nextState
					} else {
						log.Infof(
							"[pushPeerState:%s] unmounted case, returning nextState %+v as mount failed: %+v",
							f.filesystemId, nextState, responseEvent,
						)
						return nextState
					}
				}
			} else {
				log.Infof(
					"[pushPeerState] %s doesn't match target snapshot %s, "+
						"waiting for another...", sn.Id, targetSnapshot,
				)
			}
		}
	}
}
