package main

import (
	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/messaging/nats"
	"github.com/dotmesh-io/dotmesh/pkg/store"

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

type TransferPollResult = types.TransferPollResult

type Config struct {
	FilesystemMetadataTimeout int64
	UserManager               user.UserManager
	// EtcdClient                client.KeysAPI

	RegistryStore   store.RegistryStore
	FilesystemStore store.FilesystemStore
	ServerStore     store.ServerStore

	// variables used to create fsm.FsMachine
	ZFSExecPath string
	ZPoolPath   string
	PoolName    string

	// API/RPC server port
	APIServerPort string

	NatsConfig *nats.Config
}

type Prelude struct {
	SnapshotProperties []*Snapshot
}

type containerInfo struct {
	Server     string
	Containers []container.DockerContainer
}
