package main

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

func (s *InMemoryState) watchRegistryFilesystems() error {
	vals, err := s.registryStore.ListFilesystems()
	if err != nil {
		return fmt.Errorf("failed to list server addresses: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processRegistryFilesystem(val)

	}

	return s.registryStore.WatchFilesystems(idxMax, func(rf *types.RegistryFilesystem) error {
		s.processRegistryFilesystem(rf)
		return nil
	})
}

func (s *InMemoryState) processRegistryFilesystem(rf *types.RegistryFilesystem) error {
	vn := types.VolumeName{
		Namespace: rf.OwnerId,
		Name:      rf.Name,
	}

	switch rf.Meta.Action {
	case types.KVDelete:
		// deleting from the registry
		log.WithFields(log.Fields{
			"namespace": vn.Namespace,
			"name":      vn.Name,
		}).Info("[processRegistryFilesystem] deleting filesystem from registry")
		s.registry.DeleteFilesystemFromEtcd(vn)
		return nil
	case types.KVCreate, types.KVSet:
		return s.registry.UpdateFilesystemFromEtcd(vn, *rf)
	default:
		return nil
	}
}

func (s *InMemoryState) watchRegistryClones() error {
	vals, err := s.registryStore.ListClones()
	if err != nil {
		return fmt.Errorf("failed to list server addresses: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processRegistryClone(val)

	}

	return s.registryStore.WatchClones(idxMax, func(val *types.Clone) error {
		return s.processRegistryClone(val)
	})
}

func (s *InMemoryState) processRegistryClone(c *types.Clone) error {
	switch c.Meta.Action {
	case types.KVDelete:
		s.registry.DeleteCloneFromEtcd(c.Name, c.FilesystemId)
	case types.KVCreate, types.KVSet:
		s.registry.UpdateCloneFromEtcd(c.Name, c.FilesystemId, *c)
	}
	return nil
}
