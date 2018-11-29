package main

import (
	"log"
)

func discoveringState(f *fsMachine) stateFn {
	f.transitionedTo("discovering", "loading")
	log.Printf("entering discovering state for %s", f.filesystemId)

	err := f.discover()
	if err != nil {
		log.Printf("%v while discovering state", err)
		return backoffState
	}

	if !f.filesystem.exists {
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
		// TODO do we need to acquire some locks here?
		if f.filesystem.mounted {
			return activeState
		} else {
			return inactiveState
		}
	}
}
