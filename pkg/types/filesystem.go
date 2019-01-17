package types

import (
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/container"
)

type KVMeta struct {
	// KVDBIndex A Monotonically index updated at each modification operation.
	KVDBIndex uint64
	// CreatedIndex for this kv pair
	CreatedIndex uint64
	// ModifiedIndex for this kv pair
	ModifiedIndex uint64

	// Action the last action on this KVPair.
	Action KVAction
}

type FilesystemDirty struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

	NodeID     string `json:"node_id"`
	DirtyBytes int64  `json:"dirty_bytes"`
	SizeBytes  int64  `json:"size_bytes"`
}

type FilesystemMaster struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

	FilesystemID string `json:"filesystem_id"`
	NodeID       string `json:"node_id"`
}

type FilesystemDeletionAudit struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

	FilesystemID string    `json:"filesystem_id"`
	Server       string    `json:"server"`
	Username     string    `json:"username"`
	DeletedAt    time.Time `json:"deleted_at"`

	// These fields are mandatory
	Name                 VolumeName `json:"name"`
	TopLevelFilesystemId string     `json:"top_level_filesystem_id"`
	Clone                string     `json:"clone"`
}

type FilesystemLive struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

	FilesystemID string `json:"filesystem_id"`
	NodeID       string `json:"node_id"`
}

type FilesystemContainers struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

	FilesystemID string                      `json:"filesystem_id"`
	NodeID       string                      `json:"node_id"`
	Containers   []container.DockerContainer `json:"containers"`
}

// KVAction specifies the action on a KV pair. This is useful to make decisions
// from the results of  a Watch.
type KVAction int

const (
	// KVSet signifies the KV was modified.
	KVSet KVAction = 1 << iota
	// KVCreate set if the KV pair was created.
	KVCreate
	// KVGet set when the key is fetched from the KV store
	KVGet
	// KVDelete set when the key is deleted from the KV store
	KVDelete
	// KVExpire set when the key expires
	KVExpire
	// KVUknown operation on KV pair
	KVUknown
)
