package fsm

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
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

	return &types.Event{
		Name: "diffed",
		Args: &types.EventArgs{
			"files": diffFiles,
		},
	}, activeState
}

func getStringVal(vals map[string]interface{}, key string) (string, bool) {
	val, ok := vals[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	if ok {
		return "", false
	}
	return str, true
}
