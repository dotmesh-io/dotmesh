package fsm

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

func (f *FsMachine) diff(e *types.Event) (responseEvent *types.Event, nextState StateFn) {

	if e.FilesystemID == "" {
		return types.NewErrorEvent("cannot-diff", fmt.Errorf("filesystem_id not specified")), activeState
	}

	snapshotID, ok := getStringVal(*e.Args, "snapshot_id")
	if !ok {
		return types.NewErrorEvent("cannot-diff", fmt.Errorf("snapshot_id not specified")), activeState
	}

	diffFiles, err := f.zfs.Diff(e.FilesystemID, snapshotID, e.FilesystemID)
	if err != nil {
		return types.NewErrorEvent("zfs-diff-failed", fmt.Errorf("diff failed: %s", err)), activeState
	}

	encoded, err := types.EncodeZFSFileDiff(diffFiles)
	if err != nil {
		return types.NewErrorEvent("zfs-diff-encode-failed", fmt.Errorf("diff encode failed: %s", err)), activeState
	}

	return &types.Event{
		Name: "diffed",
		Args: &types.EventArgs{
			"files": encoded,
		},
	}, activeState
}

func getStringVal(vals map[string]interface{}, key string) (string, bool) {
	val, ok := vals[key]
	if !ok {
		log.WithFields(log.Fields{
			"vals": vals,
			"key":  key,
		}).Errorf("fsm diff: key is missing")
		return "", false
	}
	str, ok := val.(string)
	if !ok {
		log.WithFields(log.Fields{
			"val": val,
			"key": key,
		}).Errorf("fsm diff: value is not of type String")
		return "", false
	}
	return str, true
}
