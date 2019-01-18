package store

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"
)

// Clones

func (s *KVDBFilesystemStore) SetClone(c *types.Clone, opts *SetOptions) error {
	if c.FilesystemId == "" {
		return ErrIDNotSet
	}

	if c.Name == "" {
		return fmt.Errorf("name not set")
	}

	bts, err := s.encode(c)
	if err != nil {
		return err
	}

	_, err = s.client.Create(RegistryClonesPrefix+c.FilesystemId+"/"+c.Name, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteClone(filesystemID, cloneName string) error {
	_, err := s.client.Delete(RegistryClonesPrefix + filesystemID + "/" + cloneName)
	return err
}

func (s *KVDBFilesystemStore) WatchClones(cb WatchRegistryClonesCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchClones] error while watching KV store tree")
		}

		var c types.Clone
		err = s.decode(kvp.Value, &c)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		c.Meta = getMeta(kvp)

		return cb(&c)
	}

	return s.client.WatchTree(RegistryClonesPrefix, 0, nil, watchFunc)
}

// Registry Filesystems

func (s *KVDBFilesystemStore) SetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error {

	if f.Id == "" {
		return ErrIDNotSet
	}

	if f.OwnerId == "" {
		return fmt.Errorf("name not set")
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	_, err = s.client.Create(RegistryFilesystemsPrefix+f.Id+"/"+f.OwnerId, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) CompareAndSetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error {

	if f.Id == "" {
		return ErrIDNotSet
	}

	if f.OwnerId == "" {
		return fmt.Errorf("name not set")
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	kvp := &kvdb.KVPair{
		Key:   RegistryFilesystemsPrefix + f.Id + "/" + f.OwnerId,
		Value: bts,
	}

	_, err = s.client.CompareAndSet(kvp, opts.KVFlags, opts.PrevValue)
	return err
}

func (s *KVDBFilesystemStore) GetFilesystem(filesystemID, filesystemName string) (*types.RegistryFilesystem, error) {
	node, err := s.client.Get(RegistryFilesystemsPrefix + filesystemID + "/" + filesystemName)
	if err != nil {
		return nil, err
	}
	var f types.RegistryFilesystem
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteFilesystem(filesystemID, filesystemName string) error {
	_, err := s.client.Delete(RegistryFilesystemsPrefix + filesystemID + "/" + filesystemName)
	return err
}

func (s *KVDBFilesystemStore) CompareAndDelete(filesystemID, filesystemName string, opts *DeleteOptions) error {
	kvp := &kvdb.KVPair{
		Key:   RegistryFilesystemsPrefix + filesystemID + "/" + filesystemName,
		Value: opts.PrevValue,
	}
	_, err := s.client.CompareAndDelete(kvp, opts.KVFlags)
	return err
}

func (s *KVDBFilesystemStore) WatchFilesystems(cb WatchRegistryFilesystemsCB) error {

	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchFilesystems] error while watching KV store tree")
		}

		var f types.RegistryFilesystem
		err = s.decode(kvp.Value, &f)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		f.Meta = getMeta(kvp)

		return cb(&f)
	}

	return s.client.WatchTree(RegistryFilesystemsPrefix, 0, nil, watchFunc)
}
