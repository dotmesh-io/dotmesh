package main

import (
	"fmt"

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
		return s.processTransferPollResults(val)
	})
}

func (s *InMemoryState) processTransferPollResults(t *types.TransferPollResult) error {
	s.interclusterTransfersLock.Lock()
	defer s.interclusterTransfersLock.Unlock()

	switch t.Meta.Action {
	case types.KVDelete:
		delete(s.interclusterTransfers, t.TransferRequestId)
	case types.KVCreate, types.KVSet:
		s.interclusterTransfers[t.TransferRequestId] = *t
	}
	return nil
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
