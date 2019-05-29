package fsm

import (
	"fmt"
	"log"
	"time"
)

func backoffStateWithReason(reason string) func(f *FsMachine) StateFn {
	return func(f *FsMachine) StateFn {
		f.transitionedTo("backoff", fmt.Sprintf("pausing due to %s", reason))
		log.Printf("entering backoff state for %s", f.filesystemId)
		time.Sleep(time.Second)
		return discoveringState
	}
}

func backoffStateWithReasonCustomTimeout(reason string, timeout time.Duration) func(f *FsMachine) StateFn {
	return func(f *FsMachine) StateFn {
		f.transitionedTo("backoff", fmt.Sprintf("pausing due to %s", reason))
		log.Printf("entering backoff state for %s", f.filesystemId)
		time.Sleep(timeout)
		return discoveringState
	}
}

func backoffState(f *FsMachine) StateFn {
	f.transitionedTo("backoff", "pausing")
	log.Printf("entering backoff state for %s", f.filesystemId)
	time.Sleep(time.Second)
	return discoveringState
}
