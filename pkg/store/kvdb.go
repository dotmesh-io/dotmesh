package store

// import "github.com/portworx/kvdb"

import (
	"encoding/json"

	"github.com/dotmesh-io/dotmesh/pkg/types"
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

func (s *KVDBFilesystemStore) encode(object interface{}) ([]byte, error) {
	return json.Marshal(object)
}

func (s *KVDBFilesystemStore) decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

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
	var fm types.FilesystemMaster
	err = s.decode(node.Value, &fm)
	return &fm, err
}

func (s *KVDBFilesystemStore) DeleteMaster(id string) error {
	_, err := s.client.Delete(FilesystemMastersPrefix + id)
	return err
}

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
	return &f, err
}

func (s *KVDBFilesystemStore) DeleteDeleted(id string) error {
	_, err := s.client.Delete(FilesystemDeletedPrefix + id)
	return err
}
