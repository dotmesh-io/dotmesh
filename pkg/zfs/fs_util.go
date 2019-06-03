package zfs

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"

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

// I'm bored of writing "if snapshotId is empty"
func FullIdWithSnapshot(filesystemId, snapshotId string) string {
	if snapshotId != "" {
		return filesystemId + "@" + snapshotId
	}
	return filesystemId
}

func filterMountpoints(mountPrefix string, filesystem string, r *bufio.Reader) ([]string, error) {

	mountPrefix = filepath.Join(mountPrefix, "dmfs", filesystem)

	var mountpoints []string
	for {
		line, err := r.ReadString('\n')

		if line != "" {
			parts := strings.Split(line, " ")
			if len(parts) >= 11 {
				fsType := parts[8]
				mountpoint := parts[4]
				// mountedFS := parts[9]
				// TODO: make this filter out only _our_ filesystem mounts (e.g. snapshot mounts)
				// TODO: don't unmount the actual filesystem, seems like you
				// don't need to... just unmount the snapshot
				// "MOUNT_PREFIX=/var/lib/dotmesh/mnt",
				// 811 1024 0:203 / /var/lib/dotmesh/mnt/dmfs/0a5bb16f-0e7c-456b-9487-823634d09f13 rw,noatime shared:334 - zfs pool/dmfs/0a5bb16f-0e7c-456b-9487-823634d09f13 rw,xattr,noacl
				// 866 1024 0:204 / /var/lib/dotmesh/mnt/dmfs/8709de2a-f4c0-4d38-9241-61ca16c6764f rw,noatime shared:343 - zfs pool/dmfs/8709de2a-f4c0-4d38-9241-61ca16c6764f rw,xattr,noacl

				if fsType == "zfs" && strings.HasPrefix(mountpoint, mountPrefix) {
					mountpoints = append(mountpoints, mountpoint)
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return mountpoints, err
			}
		}
	}
	// reversing order so we get the snapshots first
	for i := len(mountpoints)/2 - 1; i >= 0; i-- {
		opp := len(mountpoints) - 1 - i
		mountpoints[i], mountpoints[opp] = mountpoints[opp], mountpoints[i]
	}

	return mountpoints, nil
}
