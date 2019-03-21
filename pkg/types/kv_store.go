package types

const DefaultEtcdURL = "https://dotmesh-etcd:42379"
const DefaultEtcdClientPort = "42379"

const EnvEtcdEndpoint = "DOTMESH_ETCD_ENDPOINT"
const EnvStorageBackend = "DOTMESH_STORAGE"
const EnvDotmeshBoltdbPath = "DOTMESH_BOLTDB_PATH"

const (
	StorageBackendEtcd   = "etcd"
	StorageBackendBoltdb = "boltdb"
)

const (
	DefaultBoltdbPath = "/data"
)
