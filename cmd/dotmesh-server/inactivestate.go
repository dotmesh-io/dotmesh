package main

import (
	"fmt"
	"log"
)

func inactiveState(f *fsMachine) stateFn {
	f.transitionedTo("inactive", "waiting")
	log.Printf("entering inactive state for %s", f.filesystemId)

	handleEvent := func(e *Event) (bool, stateFn) {
		f.transitionedTo("inactive", fmt.Sprintf("handling %s", e.Name))
		if e.Name == "delete" {
			err := f.state.deleteFilesystem(f.filesystemId)
			if err != nil {
				f.innerResponses <- &Event{
					Name: "cant-delete",
					Args: &EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &Event{
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
			f.innerResponses <- &Event{
				Name: "unmounted",
				Args: &EventArgs{},
			}
			return true, inactiveState

		} else {
			f.innerResponses <- &Event{
				Name: "unhandled",
				Args: &EventArgs{"current-state": f.currentState, "event": e},
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
	f.state.newSnapsOnMaster.Subscribe(f.filesystemId, newSnapsOnMaster)
	defer f.state.newSnapsOnMaster.Unsubscribe(f.filesystemId, newSnapsOnMaster)

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
