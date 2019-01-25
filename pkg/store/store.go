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
	WatchMasters(idx uint64, cb WatchMasterCB) error
	ListMaster() ([]*types.FilesystemMaster, error)
	ImportMasters(fs []*types.FilesystemMaster, opts *ImportOptions) error

	// /filesystems/deleted/<id>
	SetDeleted(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	GetDeleted(id string) (*types.FilesystemDeletionAudit, error)
	DeleteDeleted(id string) error
	WatchDeleted(idx uint64, cb WatchDeletedCB) error
	ListDeleted() ([]*types.FilesystemDeletionAudit, error)

	// /filesystems/cleanupPending/<id>
	SetCleanupPending(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	DeleteCleanupPending(id string) error
	// /filesystems/cleanupPending
	ListCleanupPending() ([]*types.FilesystemDeletionAudit, error)
	WatchCleanupPending(idx uint64, cb WatchCleanupPendingCB) error

	// /filesystems/live/<id>
	SetLive(fl *types.FilesystemLive, opts *SetOptions) error
	GetLive(id string) (*types.FilesystemLive, error)

	// filesystems/containers/<id>
	SetContainers(fc *types.FilesystemContainers, opts *SetOptions) error
	DeleteContainers(id string) error
	WatchContainers(idx uint64, cb WatchContainersCB) error
	ListContainers() ([]*types.FilesystemContainers, error)

	// filesystems/dirty/<id>
	SetDirty(fd *types.FilesystemDirty, opts *SetOptions) error
	DeleteDirty(id string) error
	WatchDirty(idx uint64, cb WatchDirtyCB) error
	ListDirty() ([]*types.FilesystemDirty, error)

	SetTransfer(t *types.TransferPollResult, opts *SetOptions) error
	WatchTransfers(idx uint64, cb WatchTransfersCB) error
	ListTransfers() ([]*types.TransferPollResult, error)
}

// Callbacks for filesystem events
type (
	WatchMasterCB func(fs *types.FilesystemMaster) error
	// WatchLiveCB       func(fs *types.FilesystemLive) error
	WatchContainersCB     func(fs *types.FilesystemContainers) error
	WatchDeletedCB        func(fs *types.FilesystemDeletionAudit) error
	WatchCleanupPendingCB func(fs *types.FilesystemDeletionAudit) error
	WatchDirtyCB          func(fs *types.FilesystemDirty) error
	WatchTransfersCB      func(fs *types.TransferPollResult) error
)

type RegistryStore interface {
	SetClone(c *types.Clone, opts *SetOptions) error
	DeleteClone(filesystemID, cloneName string) error
	WatchClones(idx uint64, cb WatchRegistryClonesCB) error
	ListClones() ([]*types.Clone, error)

	SetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error
	CompareAndSetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error
	GetFilesystem(namespace, filesystemName string) (*types.RegistryFilesystem, error)
	DeleteFilesystem(namespace, filesystemName string) error
	CompareAndDelete(namespace, filesystemName string, opts *DeleteOptions) error
	WatchFilesystems(idx uint64, cb WatchRegistryFilesystemsCB) error
	ListFilesystems() ([]*types.RegistryFilesystem, error)

	// Misc
	ImportClones(clones []*types.Clone, opts *ImportOptions) error
	ImportFilesystems(fs []*types.RegistryFilesystem, opts *ImportOptions) error
}

type (
	WatchRegistryClonesCB      func(c *types.Clone) error
	WatchRegistryFilesystemsCB func(f *types.RegistryFilesystem) error
)

type ServerStore interface {
	SetAddresses(si *types.Server, opts *SetOptions) error
	WatchAddresses(idx uint64, cb WatchServerAddressesClonesCB) error
	ListAddresses() ([]*types.Server, error)

	SetSnapshots(ss *types.ServerSnapshots) error
	WatchSnapshots(idx uint64, cb WatchServerSnapshotsClonesCB) error
	ListSnapshots() ([]*types.ServerSnapshots, error)

	SetState(ss *types.ServerState) error
	WatchStates(idx uint64, cb WatchServerStatesClonesCB) error
	ListStates() ([]*types.ServerState, error)
}

type (
	WatchServerAddressesClonesCB func(server *types.Server) error
	WatchServerStatesClonesCB    func(server *types.ServerState) error
	WatchServerSnapshotsClonesCB func(server *types.ServerSnapshots) error
)

type ImportOptions struct {
	DeleteExisting bool
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
