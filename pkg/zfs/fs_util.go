package zfs

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// FQ - from filesystem id to a fully qualified ZFS filesystem
func FQ(poolName, fs string) string {
	return fmt.Sprintf("%s/%s/%s", poolName, types.RootFS, fs)
}

// UnFQ - from fully qualified ZFS name to filesystem id, strip off prefix
func UnFQ(poolName, fqfs string) string {
	return fqfs[len(poolName+"/"+types.RootFS+"/"):]
}
