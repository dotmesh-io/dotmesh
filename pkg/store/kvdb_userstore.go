package store

import (
	"github.com/dotmesh-io/dotmesh/pkg/validator"
	"github.com/portworx/kvdb"
	log "github.com/sirupsen/logrus"
)

// KVStoreWithIndex is used by the UserManager to add users and then lookup them
// by their usernames
type KVStoreWithIndex interface {
	List(prefix string) ([]*kvdb.KVPair, error)
	CreateWithIndex(prefix, id, name string, val []byte) (*kvdb.KVPair, error)

	DeleteFromIndex(prefix, name string) error
	AddToIndex(prefix, name, id string) error

	Set(prefix, id string, val []byte) (*kvdb.KVPair, error)
	Get(prefix, ref string) (*kvdb.KVPair, error)
	Delete(prefix, id string) error
}

const (
	nameIndexAPIPrefix = "nameidx"
)

type NameIndex struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
	ID     string `json:"id"`
}

type KVDBStoreWithIndex struct {
	client    kvdb.Kvdb
	namespace string
}

func (s *KVDBStoreWithIndex) List(prefix string) ([]*kvdb.KVPair, error) {
	pairs, err := s.client.Enumerate(s.namespace + "/" + prefix)
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

func (s *KVDBStoreWithIndex) AddToIndex(prefix, name, id string) error {
	return s.idxAdd(prefix, name, id)
}

func (s *KVDBStoreWithIndex) DeleteFromIndex(prefix, name string) error {
	return s.idxDelete(name)
}

func (s *KVDBStoreWithIndex) CreateWithIndex(prefix, id, name string, val []byte) (*kvdb.KVPair, error) {
	resp, err := s.client.Put(s.namespace+"/"+prefix+"/"+id, val, 0)
	if err != nil {
		return nil, err
	}

	err = s.idxAdd(prefix, name, id)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"name":  name,
			"id":    id,
		}).Error("kv: failed to create index")
	}
	return resp, nil
}

func (s *KVDBStoreWithIndex) Set(prefix, id string, val []byte) (*kvdb.KVPair, error) {
	return s.client.Put(s.namespace+"/"+prefix+"/"+id, val, 0)
}

func (s *KVDBStoreWithIndex) Get(prefix, ref string) (*kvdb.KVPair, error) {
	if validator.IsUUID(ref) {
		return s.get(prefix, ref)
	}
	id, err := s.idxFindID(ref)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err,
			"prefix": prefix,
			"ref":    ref,
		}).Warn("kv: failed to find by index, getting by ref")
		// trying to get it by ref anyway
		return s.get(prefix, ref)
	}
	return s.get(prefix, id)
}

func (s *KVDBStoreWithIndex) get(prefix, id string) (*kvdb.KVPair, error) {
	return s.client.Get(s.namespace + "/" + prefix + "/" + id)

}

func (s *KVDBStoreWithIndex) Delete(prefix, id string, recursive bool) error {
	_, err := s.client.Delete(s.namespace + "/" + prefix + "/" + id)
	return err
}

func (s *KVDBStoreWithIndex) idxFindID(name string) (id string, err error) {
	var val NameIndex
	_, err = s.client.GetVal(s.namespace+"/"+nameIndexAPIPrefix+"/"+name, &val)
	if err != nil {
		return
	}
	return val.ID, nil
}

func (s *KVDBStoreWithIndex) idxAdd(prefix, name, id string) error {
	val := NameIndex{
		Prefix: prefix,
		Name:   name,
		ID:     id,
	}
	_, err := s.client.Put(s.namespace+"/"+nameIndexAPIPrefix+"/"+name, &val, 0)
	return err
}

func (s *KVDBStoreWithIndex) idxDelete(name string) error {
	_, err := s.client.Delete(s.namespace + "/" + nameIndexAPIPrefix + "/" + name)
	return err
}
