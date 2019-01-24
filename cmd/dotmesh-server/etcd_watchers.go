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

	var idxMax uin64
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

	err = s.filesystemStore.WatchMasters(idxMax, func(fm *types.FilesystemMaster) error {
		return s.processFilesystemMaster(fm)
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to start watching filesystem masters")
	}
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
