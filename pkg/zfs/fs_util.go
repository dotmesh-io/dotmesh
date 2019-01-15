package zfs

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// UnFQ - from fully qualified ZFS name to filesystem id, strip off prefix
func UnFQ(poolName, fqfs string) string {
	return fqfs[len(poolName+"/"+types.RootFS+"/"):]
}

// I'm bored of writing "if snapshotId is empty"
func FullIdWithSnapshot(filesystemId, snapshotId string) string {
	if snapshotId != "" {
		return filesystemId + "@" + snapshotId
	}
	return filesystemId
}
