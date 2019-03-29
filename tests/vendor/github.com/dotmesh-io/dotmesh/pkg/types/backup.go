package types

import "time"

type BackupV1 struct {
	Version             string                `json:"version"`
	Created             time.Time             `json:"created"`
	Users               []*User               `json:"users"`
	FilesystemMasters   []*FilesystemMaster   `json:"filesystem_masters"`
	RegistryFilesystems []*RegistryFilesystem `json:"registry_filesystems"`
	RegistryClones      []*Clone              `json:"registry_clones"`
}

const BackupVersion string = "v1"

var BackupSupportedVersions = []string{"v1"}
