package main

import (
	"log"
)

func failedState(f *fsMachine) stateFn {
	f.transitionedTo("failed", "never coming back")
	log.Printf("entering failed state for %s", f.filesystemId)
	select {}
}
