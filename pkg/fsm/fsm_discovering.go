package fsm

import (
	"fmt"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"

	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

func discoveringState(f *FsMachine) StateFn {
	f.transitionedTo("discovering", "loading")
	log.Printf("entering discovering state for %s", f.filesystemId)

	// checking whether filesystem exists in the kv store
	_, err := f.etcdClient.Get(
		context.Background(),
		fmt.Sprintf("%s/filesystems/masters/%s", types.EtcdPrefix, f.filesystemId),
		&client.GetOptions{},
	)
	if err != nil && client.IsKeyNotFound(err) {
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
