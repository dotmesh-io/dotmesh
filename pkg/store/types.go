package store

const (
	FilesystemMastersPrefix        = "/filesystems/masters/"
	FilesystemDeletedPrefix        = "/filesystems/deleted/"
	FilesystemCleanupPendingPrefix = "/filesystems/cleanupPending/"
	FilesystemLivePrefix           = "/filesystems/live/"
	FilesystemContainersPrefix     = "/filesystems/containers/"
	FilesystemDirtyPrefix          = "/filesystems/dirty/"
	FilesystemTransfersPrefix      = "/filesystems/transfers/"
)

const (
	RegistryFilesystemsPrefix = "/registry/clones/"
)

type KVType string

const (
	KVTypeEtcdV3 KVType = "etcdv3"
	KVTypeMem    KVType = "mem"
	KVTypeBolt   KVType = "bolt"
)
