package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/mock_zfs"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/golang/mock/gomock"
	"sync"
	"testing"
)

func TestFsmDiscoveringReadMetadata(t *testing.T) {
	t.Run("discover commits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockZFS := mock_zfs.NewMockZFS(ctrl)
		fsm := &FsMachine{
			filesystemId:      "hello-world",
			zfs:               mockZFS,
			snapshotsLock:     &sync.Mutex{},
			snapshotsModified: make(chan bool),
		}
		expectedFS := &types.Filesystem{
			Id:     fsm.filesystemId,
			Exists: false,
			// Important not to leave snapshots nil in the default case, we
			// need to inform other nodes that we have no snapshots of a
			// filesystem if we don't have the filesystem.
			Snapshots: []*types.Snapshot{},
		}
		mockZFS.EXPECT().DiscoverSystem(fsm.filesystemId).Return(expectedFS, nil)
		err := fsm.discover()
		if err != nil {
			t.Errorf(err.Error())
		}
	})
}
