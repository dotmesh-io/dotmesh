package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

func failedState(f *FsMachine) StateFn {
	f.transitionedTo("failed", "never coming back")
	log.Infof("entering failed state for %s", f.filesystemId)
	for {
		select {
		case _ = <-f.innerRequests:
			f.innerResponses <- &types.Event{
				Name: "failed",
				Args: &types.EventArgs{},
			}
		}
	}
}
