package fsm

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

func inactiveState(f *FsMachine) StateFn {
	f.transitionedTo("inactive", "waiting")
	log.Printf("entering inactive state for %s", f.filesystemId)

	handleEvent := func(e *types.Event) (bool, StateFn) {
		f.transitionedTo("inactive", fmt.Sprintf("handling %s", e.Name))
		if e.Name == "delete" {
			err := f.state.DeleteFilesystem(f.filesystemId)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-delete",
					Args: &types.EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &types.Event{
					Name: "deleted",
				}
			}
			return true, nil
		} else if e.Name == "mount" {
			f.transitionedTo("inactive", "mounting")
			event, nextState := f.mount()
			f.innerResponses <- event
			return true, nextState

		} else if e.Name == "unmount" {
			f.innerResponses <- &types.Event{
				Name: "unmounted",
				Args: &types.EventArgs{},
			}
			return true, inactiveState

		} else {
			f.innerResponses <- &types.Event{
				Name: "unhandled",
				Args: &types.EventArgs{"current-state": f.currentState, "event": e},
			}
			log.Printf("[inactiveState:%s] unhandled event %s", f.filesystemId, e)
		}
		return false, nil
	}

	// ensure that if there's an event on the channel which a receive was
	// cancelled in order to process, that we process that immediately before
	// going back into receive. do this with an asynchronous read before
	// checking going back into receive...
	// TODO test this behaviour

	f.transitionedTo("inactive", "waiting for requests")
	select {
	case e := <-f.innerRequests:
		doTransition, nextState := handleEvent(e)
		if doTransition {
			return nextState
		}
	default:
		// carry on
	}

	if f.attemptReceive() {
		f.transitionedTo("inactive", "found snapshots on master")
		return receivingState
	}

	newSnapsOnMaster := make(chan interface{})
	f.newSnapsOnMaster.Subscribe(f.filesystemId, newSnapsOnMaster)
	defer f.newSnapsOnMaster.Unsubscribe(f.filesystemId, newSnapsOnMaster)

	f.transitionedTo("inactive", "waiting for requests or snapshots")
	select {
	case _ = <-newSnapsOnMaster:
		return receivingState
	case e := <-f.innerRequests:
		doTransition, nextState := handleEvent(e)
		if doTransition {
			return nextState
		}
	}
	f.transitionedTo("inactive", "backing off because we don't know what else to do")
	return backoffState
}
