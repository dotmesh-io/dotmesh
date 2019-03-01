package store

// import "github.com/portworx/kvdb"

import (
	"fmt"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/bolt"
	etcdv3 "github.com/portworx/kvdb/etcd/v3"
	"github.com/portworx/kvdb/mem"
	log "github.com/sirupsen/logrus"
)

type KVDBFilesystemStore struct {
	client kvdb.Kvdb
}

type KVDBConfig struct {
	Type     KVType // etcd/bolt/mem
	Machines []string
	Options  map[string]string
	Prefix   string
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

// NewKVDBClient - returns kvdb.KVDB interface based on provided configuration
// to access KV store directly
func NewKVDBClient(cfg *KVDBConfig) (kvdb.Kvdb, error) {
	return getKVDBClient(cfg)
}

func getKVDBClient(cfg *KVDBConfig) (kvdb.Kvdb, error) {

	if cfg.Options == nil {
		cfg.Options = make(map[string]string)
	}

	switch cfg.Type {
	case KVTypeEtcdV3:
		log.WithFields(log.Fields{
			"machines": cfg.Machines,
			"options":  cfg.Options,
			"prefix":   cfg.Prefix,
		}).Info("[KVDB Client] preparing Etcd client connection...")
		return etcdv3.New(cfg.Prefix, cfg.Machines, cfg.Options, nil)
	case KVTypeMem:
		return mem.New(cfg.Prefix, []string{}, cfg.Options, nil)
	case KVTypeBolt:
		return bolt.New(cfg.Prefix, []string{}, cfg.Options, nil)
	}

	return nil, fmt.Errorf("unknown KV store type: '%s'", cfg.Type)
}

var (
	ErrNotFound = kvdb.ErrNotFound
	ErrExist    = kvdb.ErrExist
)

func IsKeyNotFound(err error) bool {
	return err == ErrNotFound
}

func IsKeyAlreadyExist(err error) bool {
	return err == ErrExist
}
