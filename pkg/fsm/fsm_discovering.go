package fsm

import (
	"log"
)

func discoveringState(f *FsMachine) StateFn {
	f.transitionedTo("discovering", "loading")
	log.Printf("entering discovering state for %s", f.filesystemId)

	err := f.discover()
	if err != nil {
		log.Printf("%v while discovering state", err)
		return backoffState
	}

	if !f.filesystem.Exists {
		return missingState
	} else {
		err := f.state.AlignMountStateWithMasters(f.filesystemId)
		if err != nil {
			log.Printf(
				"[discoveringState:%s] error trying to align mount state with masters: %v, "+
					"going into failed state forever",
				f.filesystemId,
				err,
			)
			return failedState
		}
		if f.filesystem.Mounted {
			return activeState
		} else {
			return inactiveState
		}
	}
}
