package store

// import "github.com/portworx/kvdb"

import (
	"fmt"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/bolt"
	etcdv3 "github.com/portworx/kvdb/etcd/v3"
	"github.com/portworx/kvdb/mem"
)

type KVDBFilesystemStore struct {
	client kvdb.Kvdb
}

type KVDBConfig struct {
	Type     KVType // etcd/bolt/mem
	Machines []string
}

func NewKVDBFilesystemStore(cfg *KVDBConfig) (*KVDBFilesystemStore, error) {

	client, err := getKVDBClient(cfg)
	if err != nil {
		return nil, err
	}

	return &KVDBFilesystemStore{
		client: client,
	}, nil
}

func getKVDBClient(cfg *KVDBConfig) (kvdb.Kvdb, error) {
	switch cfg.Type {
	case KVTypeEtcdV3:
		return etcdv3.New("dotmesh/", cfg.Machines, map[string]string{}, nil)
	case KVTypeMem:
		return mem.New("dotmesh/", []string{}, map[string]string{}, nil)
	case KVTypeBolt:
		return bolt.New("dotmesh/", []string{}, map[string]string{}, nil)
	}

	return nil, fmt.Errorf("unknown KV store type: '%s'", cfg.Type)
}
