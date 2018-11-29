package main

import (
	"errors"
	"fmt"
	"log"
)

var (
	ErrFilesystemDeleted = errors.New("filesystem no longer exists, it was deleted")
)

func (s *InMemoryState) GetFilesystemMachine(filesystemId string) (*fsMachine, error) {
	s.filesystemsLock.RLock()
	defer s.filesystemsLock.RUnlock()
	fsm, ok := s.filesystems[filesystemId]
	if !ok {
		return nil, fmt.Errorf("No such filesystem id %s", filesystemId)
	}
	return fsm, nil
}

func (s *InMemoryState) InitFilesystemMachine(filesystemId string) (*fsMachine, error) {
	log.Printf("[initFilesystemMachine] starting: %s", filesystemId)

	fs, deleted := func() (*fsMachine, bool) {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		// s.filesystemsLock.RLock()
		fs, ok := s.filesystems[filesystemId]
		// s.filesystemsLock.RUnlock()
		// do nothing if the fsMachine is already running
		// deleted := false
		// var err error
		if ok {
			log.Printf("[initFilesystemMachine] reusing fsMachine for %s", filesystemId)
			return fs, false
		}
		// s.filesystemsLock.Lock()
		// defer s.filesystemsLock.Unlock()

		log.Printf("[initFilesystemMachine] acquired lock: %s", filesystemId)

		fs, ok = s.filesystems[filesystemId]
		if ok {
			log.Printf("[initFilesystemMachine] reusing fsMachine for %s", filesystemId)
			return fs, false
		}

		// Don't create a new fsMachine if we've been deleted
		deleted, err := isFilesystemDeletedInEtcd(filesystemId)
		if err != nil {
			log.Printf("%v while requesting deletion state from etcd", err)
			return nil, false
		}

		if deleted {
			return nil, deleted
		}

		log.Printf("[initFilesystemMachine] initializing new fsMachine for %s", filesystemId)

		s.filesystems[filesystemId] = newFilesystemMachine(filesystemId, s)

		go s.filesystems[filesystemId].run() // concurrently run state machine
		return s.filesystems[filesystemId], deleted

	}()
	// NB: deleteFilesystem takes filesystemsLock
	if deleted {
		log.Printf("[initFilesystemMachine] deleted fsMachine found, deleting locally")
		err := s.DeleteFilesystem(filesystemId)
		if err != nil {
			log.Printf("Error deleting filesystem: %v", err)
		}
		// return nil, fmt.Errorf("No such filesystemId %s (it was deleted)", filesystemId)
		return nil, ErrFilesystemDeleted
	}
	return fs, nil
}

func (s *InMemoryState) NodeID() string {
	return s.myNodeId
}

func (s *InMemoryState) DeleteFilesystemFromMap(filesystemId string) {
	s.filesystemsLock.Lock()
	delete(s.filesystems, filesystemId)
	s.filesystemsLock.Unlock()
}

func (s *InMemoryState) DeleteFilesystem(filesystemId string) error {
	var errors []error

	log.Printf("[deleteFilesystem] Attempting to delete filesystem %s", filesystemId)

	// Remove the FS from all our myriad caches
	s.DeleteFilesystemFromMap(filesystemId)

	// Don't delete from mastersCache, because we want to be consistent wrt
	// etcd. We can wait for etcd to tell us when filesystems/masters gets
	// changed.

	s.globalContainerCacheLock.Lock()
	delete(s.globalContainerCache, filesystemId)
	s.globalContainerCacheLock.Unlock()

	// No need to worry about globalStateCache, as the fsmachine's termination will gracefully handle that

	// Ensure the toplevel filesystem's docker links are cleaned
	// up. This has to happen on every node. It only really needs to
	// happen once, when (if) we delete the "current" filesystem that
	// was checked out, but it's hard to tell when that case is so we
	// call it every time.
	err := s.cleanupDockerFilesystemState()
	if err != nil {
		errors = append(errors, err)
	}

	// Actually remove from ZFS
	err = deleteFilesystemInZFS(filesystemId)
	if err != nil {
		errors = append(errors, err)
	}

	if len(errors) != 0 {
		// We just make our best attempt at deleting; if anything
		// failed, we'll try and clean it up again later.  Therefore,
		// when we try again, various bits might already be deleted, so
		// trying to delete them fails.  It's all good.
		log.Printf("[deleteFilesystem] Errors deleting filesystem %s, possibly because some operations were previously completed: %+v", filesystemId, errors)
	}

	// However, we reserve the right to return an error if we decide to in future.
	return nil
}

func (s *InMemoryState) AlignMountStateWithMasters(filesystemId string) error {
	// We have been given a hint that a ZFS filesystem may now exist locally
	// which may need to be mounted to match up with its desired mount state
	// (as indicated by the "masters" state in etcd).
	return tryUntilSucceeds(func() error {
		fs, mounted, err := func() (*fsMachine, bool, error) {
			s.filesystemsLock.Lock()
			defer s.filesystemsLock.Unlock()

			fs, ok := s.filesystems[filesystemId]
			if !ok {
				log.Printf(
					"[AlignMountStateWithMasters] not doing anything - cannot find %v in fsMachines",
					filesystemId,
				)
				return nil, false, fmt.Errorf("cannot find %v in fsMachines", filesystemId)
			}
			masterNode, _ := s.registry.CurrentMasterNode(filesystemId)
			log.Printf(
				"[AlignMountStateWithMasters] called for %v; masterFor=%v, myNodeId=%v; mounted=%t",
				filesystemId,
				masterNode,
				s.myNodeId,
				fs.filesystem.mounted,
			)
			return fs, fs.filesystem.mounted, nil
		}()
		if err != nil {
			return err
		}

		masterNode, err := s.registry.CurrentMasterNode(filesystemId)
		if err != nil {
			return fmt.Errorf("failed to get master node for filesystem: %s", filesystemId)
		}

		// not mounted but should be (we are the master)
		if masterNode == s.myNodeId && !mounted {
			responseEvent, _ := fs.mount()
			if responseEvent.Name != "mounted" {
				return fmt.Errorf("Couldn't mount filesystem: %v", responseEvent)
			}
		}
		// mounted but shouldn't be (we are not the master)
		if masterNode != s.myNodeId && mounted {
			responseEvent, _ := fs.unmount()
			if responseEvent.Name != "unmounted" {
				return fmt.Errorf("Couldn't unmount filesystem: %v", responseEvent)
			}
		}
		return nil
	}, fmt.Sprintf("aligning mount state of %s with masters", filesystemId))
}
