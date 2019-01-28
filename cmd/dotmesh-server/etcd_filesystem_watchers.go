package main

import (
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

func (s *InMemoryState) watchFilesystemStoreMasters() error {
	fms, err := s.filesystemStore.ListMaster()
	if err != nil {
		return fmt.Errorf("failed to list filesystem masters: %s", err)
	}

	var idxMax uint64
	for _, fm := range fms {
		if fm.Meta.ModifiedIndex > idxMax {
			idxMax = fm.Meta.ModifiedIndex
		}
		err = s.processFilesystemMaster(fm)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fm.FilesystemID,
			}).Error("failed to process filesystem master")
		}
	}

	return s.filesystemStore.WatchMasters(idxMax, func(fm *types.FilesystemMaster) error {
		return s.processFilesystemMaster(fm)
	})
}

func (s *InMemoryState) processFilesystemMaster(fm *types.FilesystemMaster) error {
	switch fm.Meta.Action {
	case types.KVDelete:
		// delete
		s.registry.DeleteMasterNode(fm.FilesystemID)
		// delete(filesystemBelongsToMe, fm.FilesystemID)
	case types.KVCreate, types.KVSet:
		masterNode, ok := s.registry.GetMasterNode(fm.FilesystemID)
		if !ok || masterNode != fm.NodeID {
			log.WithFields(log.Fields{
				"filesystem_id":  fm.FilesystemID,
				"new_master":     fm.NodeID,
				"current_master": masterNode,
				"action":         fm.Meta.Action,
			}).Info("[processFilesystemMaster] updating registry master record")
			s.registry.SetMasterNode(fm.FilesystemID, fm.NodeID)

			err := s.handleFilesystemMaster(fm)
			if err != nil {
				log.WithFields(log.Fields{
					"error":           err,
					"filesystem_id":   fm.FilesystemID,
					"node_id":         fm.NodeID,
					"current_node_id": s.NodeID(),
				}).Error("error while handling filesystem master event")
			}
		}
		// filesystemBelongsToMe[fm.FilesystemID] = fm.NodeID == s.NodeID()
	}
	return nil
}

func (s *InMemoryState) handleFilesystemMaster(fm *types.FilesystemMaster) error {
	if fm.NodeID == "" {
		// The filesystem is being deleted, and we need do nothing about it
	} else {
		var err error
		var deleted bool

		deleted, err = s.isFilesystemDeletedInEtcd(fm.FilesystemID)
		if err != nil {
			log.Errorf("[handleOneFilesystemMaster] error determining if file system is deleted: fs: %s, kv store node ID: %s, error: %+v", fm.FilesystemID, fm.NodeID, err)
			return err
		}
		if deleted {
			log.Printf("[handleOneFilesystemMaster] filesystem is deleted so no need to mount/unmount fs: %s, kv store node ID: %s", fm.FilesystemID, fm.NodeID)
			// Filesystem is being deleted, so ignore it.
			return nil
		}

		_, err = s.InitFilesystemMachine(fm.FilesystemID)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fm.FilesystemID,
			}).Error("[handleOneFilesystemMaster] failed to initialize filesystem")
		}
		var responseChan chan *types.Event
		if fm.NodeID == s.NodeID() {
			log.Debugf("MOUNTING: %s=%s", fm.FilesystemID, fm.NodeID)
			responseChan, err = s.dispatchEvent(fm.FilesystemID, &types.Event{Name: "mount"}, fm.FilesystemID)
			if err != nil {
				return err
			}
		} else {
			log.Debugf("UNMOUNTING: %s=%s", fm.FilesystemID, fm.NodeID)
			responseChan, err = s.dispatchEvent(fm.FilesystemID, &types.Event{Name: "unmount"}, fm.FilesystemID)
			if err != nil {
				return err
			}
		}
		go func() {
			e := <-responseChan
			log.Debugf("[handleOneFilesystemMaster] filesystem %s response %#v", fm.FilesystemID, e)
		}()
	}
	return nil
}

func (s *InMemoryState) watchDirtyFilesystems() error {
	vals, err := s.filesystemStore.ListDirty()
	if err != nil {
		return fmt.Errorf("failed to list dirty filesystems: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processDirtyFilesystems(val)

	}

	return s.filesystemStore.WatchDirty(idxMax, func(val *types.FilesystemDirty) error {
		return s.processDirtyFilesystems(val)
	})
}

func (s *InMemoryState) processDirtyFilesystems(fd *types.FilesystemDirty) error {
	s.globalDirtyCacheLock.Lock()
	defer s.globalDirtyCacheLock.Unlock()

	switch fd.Meta.Action {
	case types.KVDelete:
		delete(s.globalDirtyCache, fd.FilesystemID)
	case types.KVCreate, types.KVSet:
		s.globalDirtyCache[fd.FilesystemID] = dirtyInfo{
			Server:     fd.NodeID,
			DirtyBytes: fd.DirtyBytes,
			SizeBytes:  fd.SizeBytes,
		}
	}
	return nil
}

func (s *InMemoryState) watchFilesystemContainers() error {
	vals, err := s.filesystemStore.ListContainers()
	if err != nil {
		return fmt.Errorf("failed to list filesystem containers: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processFilesystemContainers(val)

	}

	return s.filesystemStore.WatchContainers(idxMax, func(val *types.FilesystemContainers) error {
		return s.processFilesystemContainers(val)
	})
}

func (s *InMemoryState) processFilesystemContainers(fc *types.FilesystemContainers) error {
	s.globalContainerCacheLock.Lock()
	defer s.globalContainerCacheLock.Unlock()

	switch fc.Meta.Action {
	case types.KVDelete:
		delete(s.globalContainerCache, fc.FilesystemID)
	case types.KVCreate, types.KVSet:
		s.globalContainerCache[fc.FilesystemID] = containerInfo{
			Server:     fc.NodeID,
			Containers: fc.Containers,
		}
	}
	return nil
}

func (s *InMemoryState) watchTransfers() error {
	vals, err := s.filesystemStore.ListTransfers()
	if err != nil {
		return fmt.Errorf("failed to list filesystem containers: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processTransferPollResults(val)

	}

	return s.filesystemStore.WatchTransfers(idxMax, func(val *types.TransferPollResult) error {
		s.processTransferPollResults(val)
		return nil
	})
}

func (s *InMemoryState) processTransferPollResults(t *types.TransferPollResult) {
	s.interclusterTransfersLock.Lock()
	defer s.interclusterTransfersLock.Unlock()

	switch t.Meta.Action {
	case types.KVDelete:
		delete(s.interclusterTransfers, t.TransferRequestId)
	case types.KVCreate, types.KVSet:
		s.interclusterTransfers[t.TransferRequestId] = *t
	}
	return
}

func (s *InMemoryState) watchFilesystemDeleted() error {
	vals, err := s.filesystemStore.ListDeleted()
	if err != nil {
		return fmt.Errorf("failed to list filesystem deleted: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.handleFilesystemDeletion(val)

	}

	return s.filesystemStore.WatchDeleted(idxMax, func(val *types.FilesystemDeletionAudit) error {
		return s.handleFilesystemDeletion(val)
	})
}

// TODO: Probably remove, switched back to periodic cleanups on the node
func (s *InMemoryState) watchCleanupPending() error {
	vals, err := s.filesystemStore.ListCleanupPending()
	if err != nil {
		return fmt.Errorf("failed to list filesystem deleted: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		err = s.processFilesystemCleanup(val)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": val.FilesystemID,
			}).Error("[watchCleanupPending] got error while processing initial cleanupPending list")
		}
	}

	return s.filesystemStore.WatchCleanupPending(idxMax, func(val *types.FilesystemDeletionAudit) error {
		switch val.Meta.Action {
		case types.KVDelete:
			// nothing to do:
			return nil
		case types.KVCreate, types.KVSet:
			return s.processFilesystemCleanup(val)
		default:
			log.WithFields(log.Fields{
				"action":        val.Meta.Action,
				"filesystem_id": val.FilesystemID,
			}).Warn("[watchCleanupPending] unhandled event")
			return nil
		}

	})
}

func (s *InMemoryState) processFilesystemCleanup(fda *types.FilesystemDeletionAudit) error {
	var errors []error

	log.WithFields(log.Fields{
		"filesystem_id": fda.FilesystemID,
		"namespace":     fda.Name.Namespace,
		"name":          fda.Name.Name,
	}).Info("[processFilesystemCleanup] deleting filesystem")

	err := s.filesystemStore.DeleteContainers(fda.FilesystemID)
	if err != nil && !store.IsKeyNotFound(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fda.FilesystemID,
		}).Error("[processFilesystemCleanup] failed to delete filesystem containers during cleanup")
	}
	err = s.filesystemStore.DeleteMaster(fda.FilesystemID)
	if err != nil && !store.IsKeyNotFound(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fda.FilesystemID,
		}).Error("[processFilesystemCleanup] failed to delete filesystem master info during cleanup")
	}
	err = s.filesystemStore.DeleteDirty(fda.FilesystemID)
	if err != nil && !store.IsKeyNotFound(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fda.FilesystemID,
		}).Error("[processFilesystemCleanup] failed to delete filesystem dirty info during cleanup")
	}

	if fda.Name.Namespace != "" && fda.Name.Name != "" {
		// The name might be blank in the audit trail - this is used
		// to indicate that this was a clone, NOT the toplevel filesystem, so
		// there's no need to remove the registry entry for the whole
		// volume. We only do that when deleting the toplevel filesystem.

		// Normally, the registry entry is deleted as soon as the volume
		// is deleted, but in the event of a failure it might not have
		// been. So we try again.

		registryFilesystem, err := s.registryStore.GetFilesystem(
			fda.Name.Namespace,
			fda.Name.Name,
		)

		if err != nil {
			if store.IsKeyNotFound(err) {
				// we are good, it doesn't exist, nothing to delete
				log.WithFields(log.Fields{
					"namespace": fda.Name.Namespace,
					"name":      fda.Name.Name,
				}).Info("[processFilesystemCleanup] filesystem not found in registry, nothing to delete")
			} else {
				errors = append(errors, err)
			}
		} else {
			// We have an existing registry entry, but is it the one
			// we're supposed to delete, or a newly-created volume
			// with the name of the deleted one?

			if registryFilesystem.Id == fda.FilesystemID {
				log.WithFields(log.Fields{
					"filesystem_id": fda.FilesystemID,
					"namespace":     fda.Name.Namespace,
					"name":          fda.Name.Name,
				}).Error("[processFilesystemCleanup] deleting filesystem from registry store")
				err = s.registryStore.DeleteFilesystem(fda.Name.Namespace, fda.Name.Name)
				if err != nil && !store.IsKeyNotFound(err) {
					errors = append(errors, err)
				}
			} else {
				log.WithFields(log.Fields{
					"registry_entry_id":   registryFilesystem.Id,
					"audit_filesystem_id": fda.FilesystemID,
					"namespace":           fda.Name.Namespace,
					"name":                fda.Name.Name,
				}).Info("[processFilesystemCleanup] can't delete registry entry, IDs do not match")
			}
		}
	}

	if fda.Clone != "" {
		// The clone name might be blank in the audit trail - this is
		// used to indicate that this was the toplevel filesystem
		// rather than a clone. But when a clone name is specified,
		// we need to delete a clone record from etc.
		err = s.registryStore.DeleteClone(
			fda.TopLevelFilesystemId,
			fda.Clone,
		)
		if err != nil && !store.IsKeyNotFound(err) {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		err = s.filesystemStore.DeleteCleanupPending(fda.FilesystemID)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fda.FilesystemID,
			}).Error("[processFilesystemCleanup] failed to remove 'cleanupPending' filesystem after the cleanup")
			return fmt.Errorf("failed to remove 'cleanupPending' after the cleanup for filesystem '%s', error: %s", fda.FilesystemID, err)
		}
		log.WithFields(log.Fields{
			"filesystem_id": fda.FilesystemID,
			"namespace":     fda.Name.Namespace,
			"name":          fda.Name.Name,
		}).Info("[processFilesystemCleanup] successfuly deleted filesystem")
		return nil
	}
	return fmt.Errorf("Errors found cleaning up after a deleted filesystem: %+v", errors)

}
