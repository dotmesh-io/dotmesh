package store

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"
)

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

	_, err = s.client.Create(RegistryFilesystemsPrefix+c.FilesystemId+"/"+c.Name, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteClone(filesystemID, cloneName string) error {
	_, err := s.client.Delete(RegistryFilesystemsPrefix + filesystemID + "/" + cloneName)
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

	return s.client.WatchTree(RegistryFilesystemsPrefix, 0, nil, watchFunc)
}
