package main

import (
	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/messaging/nats"

	"github.com/coreos/etcd/client"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

const DEFAULT_BRANCH = "master"

type dirtyInfo struct {
	Server     string
	DirtyBytes int64
	SizeBytes  int64
}

type PermissionDenied struct {
}

func (e PermissionDenied) Error() string {
	return "Permission denied."
}

// Aliases
type User = user.User
type SafeUser = user.SafeUser
type CloneWithName = types.CloneWithName
type ClonesList = types.ClonesList
type TopLevelFilesystem = types.TopLevelFilesystem
type VolumesAndBranches = types.VolumesAndBranches
type Server = types.Server
type Origin = types.Origin
type PathToTopLevelFilesystem = types.PathToTopLevelFilesystem
type DotmeshVolume = types.DotmeshVolume
type VolumeName = types.VolumeName
type RegistryFilesystem = types.RegistryFilesystem

type ByAddress []Server

type dotmeshVolumeByName []DotmeshVolume

func (v dotmeshVolumeByName) Len() int {
	return len(v)
}

func (v dotmeshVolumeByName) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func (v dotmeshVolumeByName) Less(i, j int) bool {
	return v[i].Name.Name < v[j].Name.Name
}

type DotmeshVolumeAndContainers struct {
	Volume     DotmeshVolume
	Containers []container.DockerContainer
}

type VersionInfo struct {
	InstalledVersion    string `json:"installed_version"`
	CurrentVersion      string `json:"current_version"`
	CurrentReleaseDate  int    `json:"current_release_date"`
	CurrentDownloadURL  string `json:"current_download_url"`
	CurrentChangelogURL string `json:"current_changelog_url"`
	ProjectWebsite      string `json:"project_website"`
	Outdated            bool   `json:"outdated"`
}

type SafeConfig struct {
}

type Snapshot = types.Snapshot
type Clone = types.Clone
type Filesystem = types.Filesystem
type Metadata = types.Metadata

type TransferUpdateKind int

// EventArgs - alias to make refactoring easier,
// TODO: use the types directly and remove alias
type EventArgs = types.EventArgs
type Event = types.Event

type TransferPollResult struct {
	TransferRequestId string
	Peer              string // hostname
	User              string
	ApiKey            string
	Direction         string // "push" or "pull"

	// Hold onto this information, it might become useful for e.g. recursive
	// receives of clone filesystems.
	LocalNamespace   string
	LocalName        string
	LocalBranchName  string
	RemoteNamespace  string
	RemoteName       string
	RemoteBranchName string

	// Same across both clusters
	FilesystemId string

	// TODO add clusterIds? probably comes from etcd. in fact, could be the
	// discovery id (although that is only for bootstrap... hmmm).
	InitiatorNodeId string
	PeerNodeId      string

	// XXX a Transfer that spans multiple filesystem ids won't have a unique
	// starting/target snapshot, so this is in the wrong place right now.
	// although maybe it makes sense to talk about a target *final* snapshot,
	// with interim snapshots being an implementation detail.
	StartingCommit string
	TargetCommit   string

	Index              int    // i.e. transfer 1/4 (Index=1)
	Total              int    //                   (Total=4)
	Status             string // one of "starting", "running", "finished", "error"
	NanosecondsElapsed int64
	Size               int64 // size of current segment in bytes
	Sent               int64 // number of bytes of current segment sent so far
	Message            string
}

type Config struct {
	FilesystemMetadataTimeout int64
	UserManager               user.UserManager
	EtcdClient                client.KeysAPI

	// variables used to create fsm.FsMachine
	ZFSExecPath string
	ZPoolPath   string
	PoolName    string

	NatsConfig *nats.Config
}

type Prelude struct {
	SnapshotProperties []*Snapshot
}

type containerInfo struct {
	Server     string
	Containers []container.DockerContainer
}
