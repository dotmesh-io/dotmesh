package fsm

import (
	log "github.com/sirupsen/logrus"
)

func failedState(f *FsMachine) StateFn {
	f.transitionedTo("failed", "never coming back")
	log.Infof("entering failed state for %s", f.filesystemId)
	select {}
}
