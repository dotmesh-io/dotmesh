package store

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"
)

// static RegistryStore check
var _ RegistryStore = &KVDBFilesystemStore{}

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
// from RegisterFork:

// rf := types.RegistryFilesystem{
// 	Id: forkFilesystemId,
// 	// Owner is, for now, always the authenticated user at the time of
// 	// creation
// 	OwnerId:              forkName.Namespace,
// 	ForkParentId:         originFilesystemId,
// 	ForkParentSnapshotId: originSnapshotId,
// }
// _, err = r.etcdClient.Set(
// 	context.Background(),
// 	// (0)/(1)dotmesh.io/(2)registry/(3)filesystems/(4)<namespace>/(5)<name> =>
// 	//     {"Uuid": "<fs-uuid>"}
// 	fmt.Sprintf("%s/registry/filesystems/%s/%s", r.prefix, forkName.Namespace, forkName.Name),
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

	_, err = s.client.Create(RegistryFilesystemsPrefix+f.OwnerId+"/"+f.Name, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) CompareAndSetFilesystem(f *types.RegistryFilesystem, opts *SetOptions) error {

	// OwnerId == namespace
	if f.OwnerId == "" {
		return fmt.Errorf("owner ID not set")
	}

	if f.Name == "" {
		return fmt.Errorf("name not set")
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	kvp := &kvdb.KVPair{
		Key:   RegistryFilesystemsPrefix + f.OwnerId + "/" + f.Name,
		Value: bts,
	}

	_, err = s.client.CompareAndSet(kvp, opts.KVFlags, opts.PrevValue)
	if err != nil {
		return fmt.Errorf("CompareAndSetFilesystem failed, error: %s", err)
	}
	return nil
}

func (s *KVDBFilesystemStore) GetFilesystem(namespace, filesystemName string) (*types.RegistryFilesystem, error) {
	node, err := s.client.Get(RegistryFilesystemsPrefix + namespace + "/" + filesystemName)
	if err != nil {
		return nil, err
	}
	var f types.RegistryFilesystem
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteFilesystem(namespace, filesystemName string) error {
	_, err := s.client.Delete(RegistryFilesystemsPrefix + namespace + "/" + filesystemName)
	return err
}

func (s *KVDBFilesystemStore) CompareAndDelete(namespace, filesystemName string, opts *DeleteOptions) error {
	kvp := &kvdb.KVPair{
		Key:   RegistryFilesystemsPrefix + namespace + "/" + filesystemName,
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
