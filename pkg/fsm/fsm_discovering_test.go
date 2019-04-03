package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/mocks"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/golang/mock/gomock"
	"os"
	"sync"
	"testing"
)

func TestFsmDiscoveringReadMetadata(t *testing.T) {
	t.Run("discover commits", func(t *testing.T) {
		tmpDir := os.TempDir()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		os.Setenv("MOUNT_PREFIX", "mnt")
		mockZFS := mocks.NewMockZFS(ctrl)
		stateMgr := NewMockStateManager(ctrl)
		fsm := &FsMachine{
			filesystemId:      "hello-world",
			zfs:               mockZFS,
			snapshotsLock:     &sync.Mutex{},
			snapshotsModified: make(chan bool, 10),
			state:             stateMgr,
		}
		expectedFS := &types.Filesystem{
			Id:     fsm.filesystemId,
			Exists: false,
			Snapshots: []*types.Snapshot{
				&types.Snapshot{
					Id:       "commit1",
					Metadata: make(map[string]string),
				},
			},
		}
		mockZFS.EXPECT().DiscoverSystem(fsm.filesystemId).Return(expectedFS, nil)
		mockZFS.EXPECT().FQ(fsm.filesystemId).Return(tmpDir)
		stateMgr.EXPECT().NodeID().Return("node-id")
		stateMgr.EXPECT().NodeID().Return("node-id")
		stateMgr.EXPECT().UpdateSnapshotsFromKnownState(stateMgr.NodeID(), fsm.filesystemId, gomock.Any()).Return(nil)
		err := fsm.discover()
		if err != nil {
			t.Errorf(err.Error())
		}
	})
}
