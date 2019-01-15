package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/zfs"
)

// from filesystem id to a fully qualified ZFS filesystem
func fq(poolName, fs string) string {
	return zfs.FQ(poolName, fs)
}

// from fully qualified ZFS name to filesystem id, strip off prefix
func unfq(poolName, fqfs string) string {
	return zfs.UnFQ(poolName, fqfs)
}
