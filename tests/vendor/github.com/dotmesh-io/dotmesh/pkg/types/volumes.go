package types

import "fmt"

type DotmeshVolume struct {
	Id                   string
	Name                 VolumeName
	Branch               string
	Master               string
	SizeBytes            int64
	DirtyBytes           int64
	CommitCount          int64
	ServerStatuses       map[string]string // serverId => status
	ForkParentId         string
	ForkParentSnapshotId string
}

type VolumeName struct {
	Namespace string
	Name      string
}

func (v VolumeName) String() string {
	// This isn't quite a duplicate of the frontend version; on
	// the server, it's clearer to always show full namespaces and not elide admin/.
	return fmt.Sprintf("%s/%s", v.Namespace, v.Name)
}

func (v VolumeName) StringWithoutAdmin() string {
	// But, we also have the 'dm client' version, because we pass
	// that back to Docker when it asks, and use it for comparisons
	// too (e.g. when deciding which containers are using a given
	// volume).
	if v.Namespace == "admin" {
		return v.Name
	} else {
		return fmt.Sprintf("%s/%s", v.Namespace, v.Name)
	}
}
