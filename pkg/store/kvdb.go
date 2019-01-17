package store

// import "github.com/portworx/kvdb"

import (
	"github.com/dotmesh-io/dotmesh/pkg/types"

	"github.com/portworx/kvdb"
	etcdv3 "github.com/portworx/kvdb/etcd/v3"

	log "github.com/sirupsen/logrus"
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
