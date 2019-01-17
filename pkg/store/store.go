package store

import (
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// type Store interface {
// 	FilesystemSetMaster(fm *types.FilesystemMaster, ) error
// 	FilesystemGetMaster(id string) (*types.FilesystemMaster, err error)
// 	FilesystemDeleteMaster(id string) (err error)

// 	FilesystemWatchMasters()
// }

type FilesystemStore interface {
	SetMaster(fm *types.FilesystemMaster, opts *SetOptions) error
	GetMaster(id string) (*types.FilesystemMaster, error)
	DeleteMaster(id string) (err error)

	// /filesystems/deleted/<id>
	SetDeleted(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	GetDeleted(id string) (*types.FilesystemDeletionAudit, error)
	DeleteDeleted(id string) error
	// WatchDeleted(WatchDeletedCB) error

	// /filesystems/cleanupPending/<id>
	SetCleanupPending(audit *types.FilesystemDeletionAudit, opts *SetOptions) error
	DeleteCleanupPending(id string) error
	// /filesystems/cleanupPending
	ListCleanupPending() ([]*types.FilesystemDeletionAudit, error)

	// /filesystems/live/<id>
	SetLive(fl *types.FilesystemLive, opts *SetOptions) error
	GetLive(id string) (*types.FilesystemLive, error)
	WatchLive(cb WatchLiveCB) error

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
	WatchMasterCB     func(fs *types.FilesystemMaster) error
	WatchLiveCB       func(fs *types.FilesystemLive) error
	WatchContainersCB func(fs *types.FilesystemContainers) error
	WatchDeletedCB    func(fs *types.FilesystemDeletionAudit) error
	WatchDirtyCB      func(fs *types.FilesystemDirty) error
	WatchTransfersCB  func(fs *types.TransferPollResult) error
)

type SetOptions struct {
	TTL time.Duration
}
