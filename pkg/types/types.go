package types

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/user"
)

type CloneWithName struct {
	Name  string
	Clone Clone
}
type ClonesList []CloneWithName

type PermissionDenied struct {
}

func (e PermissionDenied) Error() string {
	return "Permission denied."
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

type DotmeshVolumeAndContainers struct {
	Volume     DotmeshVolume
	Containers []DockerContainer
}

type DockerContainer struct {
	Name string
	Id   string
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

type Origin struct {
	FilesystemId string
	SnapshotId   string
}

type Metadata map[string]string

type Snapshot struct {
	// exported for json serialization
	Id       string
	Metadata Metadata
	// private (do not serialize)
	// Filesystem *Filesystem
}

func (s *Snapshot) DeepCopy() *Snapshot {
	c := new(Snapshot)
	*c = *s

	meta := make(Metadata)
	for k, v := range s.Metadata {
		meta[k] = v
	}
	c.Metadata = meta

	return c
}

type Filesystem struct {
	Id        string
	Exists    bool
	Mounted   bool
	Snapshots []*Snapshot
	// support filesystem which is clone of another filesystem, for branching
	// purposes, with origin e.g. "<fs-uuid-of-actual-origin-snapshot>@<snap-id>"
	Origin Origin
}

type Clone struct {
	FilesystemId string
	Origin       Origin
}

type S3TransferRequest struct {
	KeyID           string
	SecretKey       string
	Prefixes        []string
	Endpoint        string
	Direction       string
	LocalNamespace  string
	LocalName       string
	LocalBranchName string
	RemoteName      string
}

func (transferRequest S3TransferRequest) String() string {
	v := reflect.ValueOf(transferRequest)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "SecretKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type TransferRequest struct {
	Peer             string // hostname
	User             string
	Port             int
	ApiKey           string //protected value in toString
	Direction        string // "push" or "pull"
	LocalNamespace   string
	LocalName        string
	LocalBranchName  string
	RemoteNamespace  string
	RemoteName       string
	RemoteBranchName string
	// TODO could also include SourceSnapshot here
	TargetCommit    string // optional, "" means "latest"
	StashDivergence bool
}

func (transferRequest TransferRequest) String() string {
	v := reflect.ValueOf(transferRequest)
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

type StashRequest struct {
	FilesystemId string
	SnapshotId   string
}

type EventArgs map[string]interface{}
type Event struct {
	Name string
	Args *EventArgs
}

func (ea EventArgs) String() string {
	aggr := []string{}
	for k, v := range ea {
		aggr = append(aggr, fmt.Sprintf("%s: %+q", k, v))
	}
	return strings.Join(aggr, ", ")
}

func (e Event) String() string {
	return fmt.Sprintf("<Event %s: %s>", e.Name, e.Args)
}

type Config struct {
	FilesystemMetadataTimeout int64
	UserManager               user.UserManager
}

type PathToTopLevelFilesystem struct {
	TopLevelFilesystemId   string
	TopLevelFilesystemName VolumeName
	Clones                 ClonesList
}

// the type as stored in the json in etcd (intermediate representation wrt
// DotmeshVolume)
type RegistryFilesystem struct {
	Id              string
	OwnerId         string
	CollaboratorIds []string
}

const EtcdPrefix = "/dotmesh.io"

const RootFS = "dmfs"

const MetaKeyPrefix = "io.dotmesh:meta-"

// BufLength - every 128kb of data transferred through a replication, etcd is updated with the
// amount of data and ETA and suchlike, this is used in status reporting in `dm` for example
const BufLength = 131072

// NB: It's important that the following includes characters _not_ included in
// the base64 alphabet. https://en.wikipedia.org/wiki/Base64
var EndDotmeshPrelude []byte = []byte("!!END_PRELUDE!!")

type Prelude struct {
	SnapshotProperties []*Snapshot
}

const (
	EventNameSaveFailed  = "save-failed"
	EventNameSaveSuccess = "save-success"
	EventNameReadFailed  = "read-failed"
	EventNameReadSuccess = "read-success"
)
