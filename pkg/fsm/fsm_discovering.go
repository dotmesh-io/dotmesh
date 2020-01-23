package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

func discoveringState(f *FsMachine) StateFn {
	f.transitionedTo("discovering", "loading")
	log.Printf("entering discovering state for %s", f.filesystemId)

	_, err := f.filesystemStore.GetMaster(f.filesystemId)
	if err != nil && store.IsKeyNotFound(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": f.filesystemId,
		}).Warn("[discoveringState] filesystem doesn't exist in the KV store, entering failed state forever")
		return failedState
	}

	err = f.discover()
	if err != nil {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": f.filesystemId,
		}).Error("[discoveringState] got error while discovering state")
		return missingState
	}

	if !f.filesystem.Exists {
		return missingState
	} else {
		err := f.state.AlignMountStateWithMasters(f.filesystemId)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": f.filesystemId,
			}).Error("[discoveringState] error trying to align mount state with masters, going into failed state forever")

			return failedState
		}
		if f.filesystem.Mounted {
			return activeState
		} else {
			return inactiveState
		}
	}
}

// If we initiated pull or push, we're only really finished after discovery,
// otherwise in-memory state won't match on-disk state and we can get race
// conditions due to that. So this wraps discovery and then only marks transfer
// as finished when discovery is done.
func discoveringAfterTransferInitiatorState(f *FsMachine) StateFn {
	result := discoveringState(f)
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStatus,
		Changes: types.TransferPollResult{
			Status:  "finished",
			Message: "",
		},
	}
	return result
}
