package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// random bits of code I couldn't find a good place for

func IsFilesystemMounted(fs string) (bool, error) {
	code, err := ReturnCode("mountpoint", Mnt(fs))
	if err != nil {
		return false, err
	}
	return code == 0, nil
}

func Mnt(fs string) string {
	// from filesystem id to the path it would be mounted at if it were mounted
	mountPrefix := os.Getenv("MOUNT_PREFIX")
	if mountPrefix == "" {
		panic(fmt.Sprintf("Environment variable MOUNT_PREFIX must be set\n"))
	}
	// carefully make this match...
	// MOUNT_PREFIX will be like /dotmesh-test-pools/pool_123_1/mnt
	// and we want to return
	// /dotmesh-test-pools/pool_123_1/mnt/dmfs/:filesystemId
	// fq(fs) gives pool_123_1/dmfs/:filesystemId
	// so don't use it, construct it ourselves:
	return fmt.Sprintf("%s/%s/%s", mountPrefix, types.RootFS, fs)
}

func Unmnt(p string) (string, error) {
	// From mount path to filesystem id
	mountPrefix := os.Getenv("MOUNT_PREFIX")
	if mountPrefix == "" {
		return "", fmt.Errorf("Environment variable MOUNT_PREFIX must be set\n")
	}
	if strings.HasPrefix(p, mountPrefix+"/"+types.RootFS+"/") {
		return strings.TrimPrefix(p, mountPrefix+"/"+types.RootFS+"/"), nil
	} else {
		return "", fmt.Errorf("Mount path %s does not start with %s/%s", p, mountPrefix, types.RootFS)
	}
}
