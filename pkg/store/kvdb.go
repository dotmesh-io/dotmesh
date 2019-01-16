package store

// import "github.com/portworx/kvdb"

import (
	"github.com/portworx/kvdb"
	etcdv3 "github.com/portworx/kvdb/etcd/v3"
)

type KVDBFilesystemStore struct {
	client kvdb.Kvdb
}

type KVDBConfig struct {
	Machines []string
}

func NewKVDBFilesystemStore(cfg *KVDBConfig) (*KVDBFilesystemStore, error) {

	client, err := etcdv3.New("dotmesh/", cfg.Machines, map[string]string{}, nil)
	if err != nil {
		return nil, err
	}

	return &KVDBFilesystemStore{
		client: client,
	}, nil
}
