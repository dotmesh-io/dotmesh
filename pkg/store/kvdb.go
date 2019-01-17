package store

// import "github.com/portworx/kvdb"

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"

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

// Master

// SetMaster - creates master entry only if the key didn't previously exist (using Create method)
func (s *KVDBFilesystemStore) SetMaster(fm *types.FilesystemMaster, opts *SetOptions) error {
	bts, err := s.encode(fm)
	if err != nil {
		return err
	}

	_, err = s.client.Create(FilesystemMastersPrefix+fm.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) GetMaster(id string) (*types.FilesystemMaster, error) {
	node, err := s.client.Get(FilesystemMastersPrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemMaster
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteMaster(id string) error {
	_, err := s.client.Delete(FilesystemMastersPrefix + id)
	return err
}

// Deleted

func (s *KVDBFilesystemStore) SetDeleted(f *types.FilesystemDeletionAudit, opts *SetOptions) error {
	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	_, err = s.client.Create(FilesystemDeletedPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) GetDeleted(id string) (*types.FilesystemDeletionAudit, error) {
	node, err := s.client.Get(FilesystemDeletedPrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemDeletionAudit
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteDeleted(id string) error {
	_, err := s.client.Delete(FilesystemDeletedPrefix + id)
	return err
}

// CleanupPending

func (s *KVDBFilesystemStore) SetCleanupPending(f *types.FilesystemDeletionAudit, opts *SetOptions) error {
	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	_, err = s.client.Create(FilesystemCleanupPendingPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteCleanupPending(id string) error {
	_, err := s.client.Delete(FilesystemCleanupPendingPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) ListCleanupPending() ([]*types.FilesystemDeletionAudit, error) {
	pairs, err := s.client.Enumerate(FilesystemCleanupPendingPrefix)
	if err != nil {
		return nil, err
	}
	var audits []*types.FilesystemDeletionAudit

	for _, kvp := range pairs {
		var f types.FilesystemDeletionAudit
		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Error("failed to unmarshal value into types.FilesystemDeletionAudit")
			continue
		}
		f.Meta = getMeta(kvp)
		audits = append(audits, &f)
	}

	return audits, nil
}

// Live filesystems

func (s *KVDBFilesystemStore) SetLive(f *types.FilesystemLive, opts *SetOptions) error {
	bts, err := s.encode(f)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemLivePrefix+f.FilesystemID, bts, opts.TTL)
	return err
}

func (s *KVDBFilesystemStore) GetLive(id string) (*types.FilesystemLive, error) {
	node, err := s.client.Get(FilesystemLivePrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemLive
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) SetContainers(f *types.FilesystemContainers, opts *SetOptions) error {
	bts, err := s.encode(f)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemContainersPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteContainers(id string) error {
	_, err := s.client.Delete(FilesystemContainersPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) WatchContainers(cb WatchContainersCB) error {

	// type WatchCB func(prefix string, opaque interface{}, kvp *KVPair, err error) error

	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchContainers] error while watching KV store tree")
		}

		var f types.FilesystemContainers
		err = s.decode(kvp.Value, &f)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		return cb(&f)
	}

	return s.client.WatchTree(FilesystemContainersPrefix, 0, nil, watchFunc)
}
