package main

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/statemachine"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

type User struct {
	Id       string
	Name     string
	Email    string
	Salt     []byte
	Password []byte
	ApiKey   string
	Metadata map[string]string
}

func (user User) String() string {
	v := reflect.ValueOf(user)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type SafeUser struct {
	Id        string
	Name      string
	Email     string
	EmailHash string
	Metadata  map[string]string
}

type CloneWithName struct {
	Name  string
	Clone Clone
}
type ClonesList []CloneWithName

type PathToTopLevelFilesystem struct {
	TopLevelFilesystemId   string
	TopLevelFilesystemName VolumeName
	Clones                 ClonesList
}

type Clone struct {
	FilesystemId string
	Origin       Origin
}

// refers to a clone's "pointer" to a filesystem id and its snapshot.
//
// note that a clone's Origin's FilesystemId may differ from the "top level"
// filesystemId in the Registry's Clones map if the clone is attributed to a
// top-level filesystem which is *transitively* its parent but not its direct
// parent. In this case the Origin FilesystemId will always point to its direct
// parent.

type Origin struct {
	FilesystemId string
	SnapshotId   string
}

type dirtyInfo struct {
	Server     string
	DirtyBytes int64
	SizeBytes  int64
}

type containerInfo struct {
	Server     string
	Containers []DockerContainer
}

type PermissionDenied struct {
}

func (e PermissionDenied) Error() string {
	return "Permission denied."
}

type TopLevelFilesystem struct {
	MasterBranch  DotmeshVolume
	OtherBranches []DotmeshVolume
	Owner         SafeUser
	Collaborators []SafeUser
}

type VolumesAndBranches struct {
	Dots    []TopLevelFilesystem
	Servers []Server
}

type Server struct {
	Id        string
	Addresses []string
}

type ByAddress []Server

type DotmeshVolume struct {
	Id             string
	Name           VolumeName
	Branch         string
	Master         string
	SizeBytes      int64
	DirtyBytes     int64
	CommitCount    int64
	ServerStatuses map[string]string // serverId => status
}

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
	Containers []DockerContainer
}

// A container for some state that is truly global to this process.
type InMemoryState struct {
	config                     Config
	filesystems                map[string]*fsMachine
	filesystemsLock            *sync.RWMutex
	myNodeId                   string
	mastersCache               map[string]string
	mastersCacheLock           *sync.RWMutex
	serverAddressesCache       map[string]string
	serverAddressesCacheLock   *sync.RWMutex
	globalSnapshotCache        map[string]map[string][]snapshot
	globalSnapshotCacheLock    *sync.RWMutex
	globalStateCache           map[string]map[string]map[string]string
	globalStateCacheLock       *sync.RWMutex
	globalContainerCache       map[string]containerInfo
	globalContainerCacheLock   *sync.RWMutex
	etcdWaitTimestamp          int64
	etcdWaitState              string
	etcdWaitTimestampLock      *sync.Mutex
	localReceiveProgress       *Observer
	newSnapsOnMaster           *Observer
	registry                   *Registry
	containers                 *DockerClient
	containersLock             *sync.RWMutex
	fetchRelatedContainersChan chan bool
	interclusterTransfers      map[string]TransferPollResult
	interclusterTransfersLock  *sync.RWMutex
	globalDirtyCacheLock       *sync.RWMutex
	globalDirtyCache           map[string]dirtyInfo
	userManager                user.UserManager

	debugPartialFailCreateFilesystem bool
	versionInfo                      *VersionInfo
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

// type fsMap map[string]*fsMachine

type metadata map[string]string
type snapshot struct {
	// exported for json serialization
	Id       string
	Metadata *metadata
	// private (do not serialize)
	filesystem *filesystem
}

type filesystem struct {
	id        string
	exists    bool
	mounted   bool
	snapshots []*snapshot
	// support filesystem which is clone of another filesystem, for branching
	// purposes, with origin e.g. "<fs-uuid-of-actual-origin-snapshot>@<snap-id>"
	origin Origin
}

func castToMetadata(val interface{}) metadata {
	meta, ok := val.(metadata)
	if !ok {
		meta = metadata{}
		// massage the data into the right type
		cast := val.(map[string]interface{})
		for k, v := range cast {
			meta[k] = v.(string)
		}
	}
	return meta
}

type Prelude struct {
	SnapshotProperties []*snapshot
}

type transferFn func(
	f *fsMachine,
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	transferRequestId string, pollResult *TransferPollResult,
	client *JsonRpcClient, transferRequest *TransferRequest,
) (*Event, stateFn)

// Defaults are specified in main.go
type Config struct {
	FilesystemMetadataTimeout int64
	UserManager               user.UserManager
}

type SafeConfig struct {
}

type VolumeName struct {
	Namespace string
	Name      string
}
