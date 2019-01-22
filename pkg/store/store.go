package store

import (
	"errors"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"
)

type FilesystemStore interface {
	SetMaster(fm *types.FilesystemMaster, opts *SetOptions) error
	CompareAndSetMaster(fm *types.FilesystemMaster, opts *SetOptions) error
	GetMaster(id string) (*types.FilesystemMaster, error)
	DeleteMaster(id string) (err error)

	// /filesystems/deleted/<id>
	SetDeleted(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	GetDeleted(id string) (*types.FilesystemDeletionAudit, error)
	DeleteDeleted(id string) error
	WatchDeleted(cb WatchDeletedCB) error

	// /filesystems/cleanupPending/<id>
	SetCleanupPending(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	DeleteCleanupPending(id string) error
	// /filesystems/cleanupPending
	ListCleanupPending() ([]*types.FilesystemDeletionAudit, error)

	// /filesystems/live/<id>
	SetLive(fl *types.FilesystemLive, opts *SetOptions) error
	GetLive(id string) (*types.FilesystemLive, error)

	// filesystems/containers/<id>
	SetContainers(fc *types.FilesystemContainers, opts *SetOptions) error
	DeleteContainers(id string) error
	WatchContainers(cb WatchContainersCB) error

	// filesystems/dirty/<id>
	SetDirty(fd *types.FilesystemDirty, opts *SetOptions) error
	DeleteDirty(id string) error
	WatchDirty(cb WatchDirtyCB) error

	SetTransfer(t *types.TransferPollResult, opts *SetOptions) error
	WatchTransfers(cb WatchTransfersCB) error
}

// Callbacks for filesystem events
type (
	WatchMasterCB func(fs *types.FilesystemMaster) error
	// WatchLiveCB       func(fs *types.FilesystemLive) error
	WatchContainersCB func(fs *types.FilesystemContainers) error
	WatchDeletedCB    func(fs *types.FilesystemDeletionAudit) error
	WatchDirtyCB      func(fs *types.FilesystemDirty) error
	WatchTransfersCB  func(fs *types.TransferPollResult) error
)

type RegistryStore interface {
	SetClone(c *types.Clone, opts *SetOptions) error
	DeleteClone(filesystemID, cloneName string) error
	WatchClones(cb WatchRegistryClonesCB) error

	SetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error
	CompareAndSetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error
	GetFilesystem(namespace, filesystemName string) (*types.RegistryFilesystem, error)
	DeleteFilesystem(namespace, filesystemName string) error
	CompareAndDelete(namespace, filesystemName string, opts *DeleteOptions) error
	WatchFilesystems(cb WatchRegistryFilesystemsCB) error
}

type (
	WatchRegistryClonesCB      func(c *types.Clone) error
	WatchRegistryFilesystemsCB func(f *types.RegistryFilesystem) error
)

type ServerStore interface {
	SetAddresses(si *types.Server, opts *SetOptions) error
	ListAddresses() ([]*types.Server, error)
	SetSnapshots(ss *types.ServerSnapshots) error
	SetState(ss *types.ServerState) error
}

type SetOptions struct {
	// TTL seconds
	TTL       uint64
	PrevValue []byte
	KVFlags   kvdb.KVFlags
	Force     bool
}

type DeleteOptions struct {
	PrevValue []byte
	KVFlags   kvdb.KVFlags
}

var (
	ErrIDNotSet = errors.New("ID not set")
)
