package main

import (
	"fmt"
	"strconv"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

func (s *InMemoryState) watchServerAddresses() error {
	vals, err := s.serverStore.ListAddresses()
	if err != nil {
		return fmt.Errorf("failed to list server addresses: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processServerAddress(val)

	}

	return s.serverStore.WatchAddresses(idxMax, func(srv *types.Server) error {
		s.processServerAddress(srv)
		return nil
	})
}

func (s *InMemoryState) processServerAddress(srv *types.Server) {
	s.serverAddressesCacheLock.Lock()
	s.serverAddressesCache[srv.Id] = srv.Addresses
	s.serverAddressesCacheLock.Unlock()
}

func (s *InMemoryState) watchServerStates() error {
	vals, err := s.serverStore.ListStates()
	if err != nil {
		return fmt.Errorf("failed to list server states: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processServerState(val)
	}

	return s.serverStore.WatchStates(idxMax, func(ss *types.ServerState) error {
		return s.processServerState(ss)
	})
}

func (s *InMemoryState) processServerState(ss *types.ServerState) error {
	switch ss.Meta.Action {
	case types.KVDelete:
		// nothing to do
		return nil
	case types.KVGet, types.KVCreate, types.KVSet:
		fsm, err := s.InitFilesystemMachine(ss.FilesystemID)
		if err != nil {
			return err
		}

		currentMeta := fsm.GetMetadata(ss.ID)
		currentVersion, ok := currentMeta["version"]
		if ok {
			i, err := strconv.ParseUint(currentVersion, 10, 64)
			if err != nil {
				log.WithFields(log.Fields{
					"error":                 err,
					"current_version_entry": currentMeta["version"],
				}).Error("watch states: failed to parse current modified idx entry")
			} else {
				if i > ss.Meta.ModifiedIndex {
					log.Printf(
						"Out of order updates! %s is older than %s",
						currentMeta,
						ss.ID,
					)
					return nil
				}
			}

		}
		ss.State["version"] = fmt.Sprintf("%d", ss.Meta.ModifiedIndex)
		fsm.SetMetadata(ss.ID, ss.State)

		return nil
	default:
		// not interested
		return nil
	}
}

func (s *InMemoryState) watchServerSnapshots() error {
	vals, err := s.serverStore.ListSnapshots()
	if err != nil {
		return fmt.Errorf("failed to list server snapshots: %s", err)
	}

	var idxMax uint64
	for _, val := range vals {
		if val.Meta.ModifiedIndex > idxMax {
			idxMax = val.Meta.ModifiedIndex
		}
		s.processServerSnapshots(val)
	}

	return s.serverStore.WatchSnapshots(idxMax, func(ss *types.ServerSnapshots) error {
		return s.processServerSnapshots(ss)
	})
}

func (s *InMemoryState) processServerSnapshots(ss *types.ServerSnapshots) error {
	switch ss.Meta.Action {
	case types.KVDelete:
		return s.UpdateSnapshotsFromKnownState(ss.ID, ss.FilesystemID, []*Snapshot{})
	case types.KVGet, types.KVCreate, types.KVSet:
		return s.UpdateSnapshotsFromKnownState(ss.ID, ss.FilesystemID, ss.Snapshots)
	default:
		return nil
	}
}
