package store

const (
	FilesystemMastersPrefix        = "/filesystems/masters/"
	FilesystemDeletedPrefix        = "/filesystems/deleted/"
	FilesystemCleanupPendingPrefix = "/filesystems/cleanupPending/"
	FilesystemLivePrefix           = "/filesystems/live/"
	FilesystemContainersPrefix     = "/filesystems/containers/"
)

type KVType string

const (
	KVTypeEtcdV3 KVType = "etcdv3"
	KVTypeMem    KVType = "mem"
	KVTypeBolt   KVType = "bolt"
)
