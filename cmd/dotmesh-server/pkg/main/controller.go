package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
)

// typically methods on the InMemoryState "god object"

func NewInMemoryState(localPoolId string, config Config) *InMemoryState {
	d, err := NewDockerClient()
	if err != nil {
		panic(err)
	}
	s := &InMemoryState{
		config:          config,
		filesystems:     &fsMap{},
		filesystemsLock: &sync.Mutex{},
		myNodeId:        localPoolId,
		// filesystem => node id
		mastersCache:     &map[string]string{},
		mastersCacheLock: &sync.Mutex{},
		// server id => comma-separated IPv[46] addresses
		serverAddressesCache:     &map[string]string{},
		serverAddressesCacheLock: &sync.Mutex{},
		// server id => filesystem id => snapshot metadata
		globalSnapshotCache:     &map[string]map[string][]snapshot{},
		globalSnapshotCacheLock: &sync.Mutex{},
		// server id => filesystem id => state machine metadata
		globalStateCache:     &map[string]map[string]map[string]string{},
		globalStateCacheLock: &sync.Mutex{},
		// global container state (what containers are running where), filesystemId -> containerInfo
		globalContainerCache:     &map[string]containerInfo{},
		globalContainerCacheLock: &sync.Mutex{},
		// When did we start waiting for etcd?
		etcdWaitTimestamp:     0,
		etcdWaitState:         "",
		etcdWaitTimestampLock: &sync.Mutex{},
		// a sort of global event bus for filesystems getting new snapshots on
		// their masters, keyed on filesystem name, which interested parties
		// such as slaves for that filesystem may subscribe to
		newSnapsOnMaster:     NewObserver(),
		localReceiveProgress: NewObserver(),
		// containers that are running with dotmesh volumes by filesystem id
		containers:     d,
		containersLock: &sync.Mutex{},
		// channel to send on to hint that a new container is using a dotmesh
		// volume
		fetchRelatedContainersChan: make(chan bool),
		// inter-cluster transfers are recorded here
		interclusterTransfers:     &map[string]TransferPollResult{},
		interclusterTransfersLock: &sync.Mutex{},
		globalDirtyCacheLock:      &sync.Mutex{},
		globalDirtyCache:          &map[string]dirtyInfo{},
		versionInfo:               &VersionInfo{InstalledVersion: serverVersion},
	}
	// a registry of names of filesystems and branches (clones) mapping to
	// their ids
	s.registry = NewRegistry(s)
	return s
}

func (s *InMemoryState) resetRegistry() {
	s.registry = NewRegistry(s)
}

func (s *InMemoryState) deleteFilesystem(filesystemId string) error {
	var errors []error

	log.Printf("[deleteFilesystem] Attempting to delete filesystem %s", filesystemId)

	// Remove the FS from all our myriad caches
	func() {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		delete(*s.filesystems, filesystemId)
	}()

	// Don't delete from mastersCache, because we want to be consistent wrt
	// etcd. We can wait for etcd to tell us when filesystems/masters gets
	// changed.

	func() {
		s.globalContainerCacheLock.Lock()
		defer s.globalContainerCacheLock.Unlock()
		delete(*s.globalContainerCache, filesystemId)
	}()

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

func (s *InMemoryState) alignMountStateWithMasters(filesystemId string) error {
	// We have been given a hint that a ZFS filesystem may now exist locally
	// which may need to be mounted to match up with its desired mount state
	// (as indicated by the "masters" state in etcd).

	fs, mounted, err := func() (*fsMachine, bool, error) {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()

		fs, ok := (*s.filesystems)[filesystemId]
		if !ok {
			log.Printf(
				"[alignMountStateWithMasters] not doing anything - cannot find %v in fsMachines",
				filesystemId,
			)
			return nil, false, fmt.Errorf("cannot find %v in fsMachines", filesystemId)
		}
		log.Printf(
			"[alignMountStateWithMasters] called for %v; masterFor=%v, myNodeId=%v; mounted=%b",
			filesystemId,
			s.masterFor(filesystemId),
			s.myNodeId,
			fs.filesystem.mounted,
		)
		return fs, fs.filesystem.mounted, nil
	}()
	if err != nil {
		return err
	}

	// not mounted but should be (we are the master)
	if s.masterFor(filesystemId) == s.myNodeId && !mounted {
		responseEvent, _ := fs.mount()
		if responseEvent.Name != "mounted" {
			return fmt.Errorf("Couldn't mount filesystem: %v", responseEvent)
		}
	}
	// mounted but shouldn't be (we are not the master)
	if s.masterFor(filesystemId) != s.myNodeId && mounted {
		responseEvent, _ := fs.unmount()
		if responseEvent.Name != "unmounted" {
			return fmt.Errorf("Couldn't unmount filesystem: %v", responseEvent)
		}
	}
	return nil
}

func (s *InMemoryState) calculatePrelude(toFilesystemId, toSnapshotId string) (Prelude, error) {
	var prelude Prelude
	snaps, err := s.snapshotsFor(s.myNodeId, toFilesystemId)
	if err != nil {
		return prelude, err
	}
	pointerSnaps := []*snapshot{}
	for _, s := range snaps {
		// Take a copy of s to take a pointer of, rather than getting
		// lots of pointers to so in the pointerSnaps slice...
		snapshots := s
		pointerSnaps = append(pointerSnaps, &snapshots)
	}

	prelude.SnapshotProperties, err = restrictSnapshots(pointerSnaps, toSnapshotId)
	if err != nil {
		return prelude, err
	}
	return prelude, nil
}

func (s *InMemoryState) getOne(ctx context.Context, fs string) (DotmeshVolume, error) {
	// TODO simplify this by refactoring it into multiple functions,
	// simplifying locking in the process.
	master, err := s.currentMaster(fs)
	if err != nil {
		return DotmeshVolume{}, err
	}

	quietLogger(fmt.Sprintf("[getOne] starting for %v", fs))

	if tlf, clone, err := s.registry.LookupFilesystemById(fs); err == nil {
		authorized, err := tlf.Authorize(ctx)
		if err != nil {
			return DotmeshVolume{}, err
		}
		if !authorized {
			quietLogger(fmt.Sprintf(
				"[getOne] notauth for %v", fs,
			))
			return DotmeshVolume{}, PermissionDenied{}
		}
		// if not exists, 0 is fine
		s.globalDirtyCacheLock.Lock()
		quietLogger(fmt.Sprintf(
			"[getOne] looking up %s with master %s in %s",
			fs, master, *s.globalDirtyCache,
		))
		dirty, ok := (*s.globalDirtyCache)[fs]
		var dirtyBytes int64
		var sizeBytes int64
		if ok {
			dirtyBytes = dirty.DirtyBytes
			sizeBytes = dirty.SizeBytes
			quietLogger(fmt.Sprintf(
				"[getOne] got dirtyInfo %d,%d for %s with master %s in %s",
				sizeBytes, dirtyBytes, fs, master, *s.globalDirtyCache,
			))
		} else {
			quietLogger(fmt.Sprintf(
				"[getOne] %s was not in %s",
				fs, *s.globalDirtyCache,
			))
		}
		s.globalDirtyCacheLock.Unlock()
		// if not exists, 0 is fine
		s.globalSnapshotCacheLock.Lock()
		snapshots, ok := (*s.globalSnapshotCache)[master][fs]
		s.globalSnapshotCacheLock.Unlock()
		var commitCount int64
		if ok {
			commitCount = int64(len(snapshots))
		}

		d := DotmeshVolume{
			Name:           tlf.MasterBranch.Name,
			Branch:         clone,
			Master:         master,
			DirtyBytes:     dirtyBytes,
			SizeBytes:      sizeBytes,
			Id:             fs,
			CommitCount:    commitCount,
			ServerStatuses: map[string]string{},
		}
		s.serverAddressesCacheLock.Lock()
		defer s.serverAddressesCacheLock.Unlock()

		servers := []Server{}
		for server, addresses := range *s.serverAddressesCache {
			servers = append(servers, Server{
				Id: server, Addresses: strings.Split(addresses, ","),
			})
		}
		sort.Sort(ByAddress(servers))
		for _, server := range servers {
			// get current state and status for filesystem on server from our
			// cache
			s.globalSnapshotCacheLock.Lock()
			numSnapshots := len((*s.globalSnapshotCache)[server.Id][fs])
			s.globalSnapshotCacheLock.Unlock()
			s.globalStateCacheLock.Lock()
			state, ok := (*s.globalStateCache)[server.Id][fs]
			status := ""
			if !ok {
				status = fmt.Sprintf("unknown, %d snaps", numSnapshots)
			} else {
				status = fmt.Sprintf(
					"%s: %s, %d snaps (v%s)",
					state["state"], state["status"],
					numSnapshots, state["version"],
				)
			}
			d.ServerStatuses[server.Id] = status
			s.globalStateCacheLock.Unlock()
		}
		quietLogger(fmt.Sprintf(
			"[getOne] here is your volume: %s", d,
		))
		return d, nil
	} else {
		return DotmeshVolume{}, fmt.Errorf("Unable to find filesystem name for id %s", fs)
	}
}

func (s *InMemoryState) notifyPushCompleted(filesystemId string, success bool) {
	s.filesystemsLock.Lock()
	f, ok := (*s.filesystems)[filesystemId]
	s.filesystemsLock.Unlock()
	if !ok {
		log.Printf("[notifyPushCompleted] No such filesystem id %s", filesystemId)
		return
	}
	log.Printf("[notifyPushCompleted:%s] about to notify chan with success=%t", filesystemId, success)
	f.pushCompleted <- success
	log.Printf("[notifyPushCompleted:%s] done notify chan", filesystemId)
}

func (s *InMemoryState) getCurrentState(filesystemId string) (string, error) {
	// init fsMachine in case it isn't.
	// XXX this trusts (authenticated) POST data :/
	s.initFilesystemMachine(filesystemId)

	s.filesystemsLock.Lock()
	defer s.filesystemsLock.Unlock()
	f, ok := (*s.filesystems)[filesystemId]
	if !ok {
		return "", fmt.Errorf("No such filesystem id %s", filesystemId)
	}
	return f.getCurrentState(), nil
}

func (s *InMemoryState) insertInitialAdminPassword() error {

	if os.Getenv("INITIAL_ADMIN_PASSWORD") == "" ||
		os.Getenv("INITIAL_ADMIN_API_KEY") == "" {
		log.Printf("INITIAL_ADMIN_PASSWORD and INITIAL_ADMIN_API_KEY are required in order to create an admin user")
		return nil
	}

	adminPassword, err := base64.StdEncoding.DecodeString(
		os.Getenv("INITIAL_ADMIN_PASSWORD"),
	)
	if err != nil {
		return err
	}

	adminKey, err := base64.StdEncoding.DecodeString(
		os.Getenv("INITIAL_ADMIN_API_KEY"),
	)
	if err != nil {
		return err
	}

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	salt, hashedPassword, err := HashPassword(string(adminPassword))
	if err != nil {
		return err
	}

	user := struct {
		Id       string
		Name     string
		Salt     []byte
		Password []byte
		ApiKey   string
	}{Id: ADMIN_USER_UUID, Name: "admin", Salt: salt, Password: hashedPassword, ApiKey: string(adminKey)}
	encoded, err := json.Marshal(user)
	if err != nil {
		return err
	}

	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("/dotmesh.io/users/%s", ADMIN_USER_UUID),
		string(encoded),
		&client.SetOptions{PrevExist: client.PrevNoExist},
	)
	return err

}

// query container runtime for any containers which have dotmesh volumes.
// update etcd with our findings, so that other servers can learn about what
// containers we've got running here (for purposes of displaying this
// information in 'dm list', etc).
//
// TODO hold the containersLock throughout the iteration, so that any requests
// from a container runtime (e.g. docker) via its plugin mechanism to provision
// a volume that would interact with this state will wait until we've finished
// updating our internal state (and the etcd state).
func (s *InMemoryState) fetchRelatedContainers() error {
	for {
		err := s.findRelatedContainers()
		if err != nil {
			return err
		}
		// wait for the next hint that containers have changed
		_ = <-s.fetchRelatedContainersChan
	}
}

func (s *InMemoryState) findRelatedContainers() error {
	s.containersLock.Lock()
	defer s.containersLock.Unlock()
	containerMap, err := s.containers.AllRelated()
	if err != nil {
		return err
	}
	log.Printf("findRelatedContainers got containerMap %s", containerMap)
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	// Iterate over _every_ filesystem id we know we are masters for on this
	// system, zeroing out the etcd record of containers running on that
	// filesystem unless we just learned about them. (This means that when a
	// container stops, it no longer shows as running.)

	myFilesystems := []string{}
	s.mastersCacheLock.Lock()
	for filesystemId, master := range *s.mastersCache {
		if s.myNodeId == master {
			myFilesystems = append(myFilesystems, filesystemId)
		}
	}
	s.mastersCacheLock.Unlock()

	log.Printf("findRelatedContainers with containerMap %s, myFilesystems %s", containerMap, myFilesystems)

	for _, filesystemId := range myFilesystems {
		// update etcd with the list of containers and this node; we'll learn
		// about the state via our own watch on etcd
		// (0)/(1)dotmesh.io/(2)filesystems/(3)containers/(4):filesystem_id =>
		// {"server": "server", "containers": [{Name: "name", ID: "id"}]}
		theContainers, ok := containerMap[filesystemId]
		var value containerInfo
		if ok {
			value = containerInfo{
				Server:     s.myNodeId,
				Containers: theContainers,
			}
		} else {
			value = containerInfo{
				Server:     s.myNodeId,
				Containers: []DockerContainer{},
			}
		}
		result, err := json.Marshal(value)
		if err != nil {
			return err
		}

		// update our local globalContainerCache immediately, so that we reduce
		// the window for races against setting this cache value.
		func() {
			s.globalContainerCacheLock.Lock()
			defer s.globalContainerCacheLock.Unlock()
			(*s.globalContainerCache)[filesystemId] = value
		}()

		log.Printf(
			"findRelatedContainers setting %s to %s",
			fmt.Sprintf("%s/filesystems/containers/%s", ETCD_PREFIX, filesystemId),
			string(result),
		)
		_, err = kapi.Set(
			context.Background(),
			fmt.Sprintf("%s/filesystems/containers/%s", ETCD_PREFIX, filesystemId),
			string(result),
			nil,
		)
	}
	return nil
}

func (s *InMemoryState) currentMaster(filesystemId string) (string, error) {
	s.mastersCacheLock.Lock()
	defer s.mastersCacheLock.Unlock()

	master, ok := (*s.mastersCache)[filesystemId]
	if !ok {
		return "", fmt.Errorf("No known filesystem with id %s", filesystemId)
	}
	return master, nil
}

func (s *InMemoryState) snapshotsForCurrentMaster(filesystemId string) ([]snapshot, error) {
	master, err := s.currentMaster(filesystemId)
	if err != nil {
		return []snapshot{}, err
	}
	return s.snapshotsFor(master, filesystemId)
}

func (s *InMemoryState) snapshotsFor(server string, filesystemId string) ([]snapshot, error) {
	s.globalSnapshotCacheLock.Lock()
	defer s.globalSnapshotCacheLock.Unlock()
	filesystems, ok := (*s.globalSnapshotCache)[server]
	if !ok {
		return []snapshot{}, nil
	}
	snapshots, ok := filesystems[filesystemId]
	if !ok {
		return []snapshot{}, nil
	}
	return snapshots, nil
}

// the addresses of a named server id
func (s *InMemoryState) addressesFor(server string) []string {
	s.serverAddressesCacheLock.Lock()
	defer s.serverAddressesCacheLock.Unlock()
	addresses, ok := (*s.serverAddressesCache)[server]
	if !ok {
		// don't know about this server
		// TODO maybe this should be an error
		return []string{}
	}
	return strings.Split(addresses, ",")
}

func (s *InMemoryState) masterFor(filesystem string) string {
	s.mastersCacheLock.Lock()
	defer s.mastersCacheLock.Unlock()
	currentMaster, ok := (*s.mastersCache)[filesystem]
	if !ok {
		// don't know about this filesystem
		// TODO maybe this should be an error
		return ""
	}
	return currentMaster
}

func (s *InMemoryState) initFilesystemMachine(filesystemId string) *fsMachine {
	log.Printf("[initFilesystemMachine] starting: %s", filesystemId)

	fs, deleted := func() (*fsMachine, bool) {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		fs, ok := (*s.filesystems)[filesystemId]
		log.Printf("[initFilesystemMachine] acquired lock: %s", filesystemId)
		// do nothing if the fsMachine is already running
		deleted := false
		var err error
		if ok {
			log.Printf("[initFilesystemMachine] reusing fsMachine for %s", filesystemId)
			return fs, false
		} else {
			// Don't create a new fsMachine if we've been deleted
			deleted, err = isFilesystemDeletedInEtcd(filesystemId)
			if err != nil {
				log.Printf("%v while requesting deletion state from etcd", err)
				return nil, false
			}
		}
		if !deleted {
			log.Printf("[initFilesystemMachine] initializing new fsMachine for %s", filesystemId)
			(*s.filesystems)[filesystemId] = newFilesystemMachine(filesystemId, s)
			go (*s.filesystems)[filesystemId].run() // concurrently run state machine
			return (*s.filesystems)[filesystemId], deleted
		} else {
			return fs, deleted
		}
	}()
	// NB: deleteFilesystem takes filesystemsLock
	if deleted {
		err := s.deleteFilesystem(filesystemId)
		if err != nil {
			log.Printf("Error deleting filesystem: %v", err)
		}
		return nil
	}
	return fs
}

func (s *InMemoryState) exists(filesystem string) bool {
	s.filesystemsLock.Lock()
	defer s.filesystemsLock.Unlock()
	_, ok := (*s.filesystems)[filesystem]
	return ok
}

// return a filesystem or error
func (s *InMemoryState) maybeFilesystem(filesystemId string) (*fsMachine, error) {
	fs := s.initFilesystemMachine(filesystemId)
	if fs == nil {
		// It was deleted.
		return nil, fmt.Errorf("No such filesystemId %s (it was deleted)", filesystemId)
	}
	return fs, nil
}

func (state *InMemoryState) reallyProcureFilesystem(ctx context.Context, name VolumeName) (
	string, error,
) {
	// move filesystem here if it's not here already (coordinate the move
	// with the current master via etcd), also (TODO check this) DON'T
	// ALLOW PATH TO BE PASSED TO DOCKER IF IT IS NOT ACTUALLY MOUNTED
	// (otherwise databases will show up as empty)

	// If the filesystem exists anywhere in the cluster, and a small amount
	// of time has passed, we should have an inactive filesystem state
	// machine.

	cloneName := ""
	if strings.Contains(name.Name, "@") {
		shrapnel := strings.Split(name.Name, "@")
		name.Name = shrapnel[0]
		cloneName = shrapnel[1]
		if cloneName == DEFAULT_BRANCH {
			cloneName = ""
		}
	}

	log.Printf(
		"*** Attempting to procure filesystem name %s and clone name %s",
		name, cloneName,
	)

	filesystemId, err := state.registry.MaybeCloneFilesystemId(name, cloneName)
	if err == nil {
		// TODO can we synchronize with the state machine somehow, to
		// ensure that we're not currently on a master in the process of
		// doing a handoff?
		master := state.masterFor(filesystemId)
		if master == state.myNodeId {
			log.Printf("Volume already here, we are done %s", filesystemId)
			return filesystemId, nil
		} else if master == "" {
			return "", fmt.Errorf("Internal error: The volume name exists, but the volume does not (have a master). Name:%s Clone:%s ID:%s", name, cloneName, filesystemId)
		} else {
			// put in a request for the current master of the filesystem to
			// move it to me
			responseChan, err := state.globalFsRequest(
				filesystemId,
				&Event{
					Name: "move",
					Args: &EventArgs{"target": state.myNodeId},
				},
			)
			if err != nil {
				return "", err
			}
			log.Printf(
				"Attempting to move %s from %s to me (%s)",
				filesystemId,
				state.masterFor(filesystemId),
				state.myNodeId,
			)
			var e *Event
			select {
			case <-time.After(30 * time.Second):
				// something needs to read the response from the
				// response chan
				go func() { _ = <-responseChan }()
				// TODO implement some kind of liveness check to avoid
				// timing out too early on slow transfers.
				return "", fmt.Errorf(
					"timed out trying to procure %s, please try again", filesystemId,
				)
			case e = <-responseChan:
				// tally ho!
			}
			log.Printf(
				"Attempting to move %s from %s to me (%s)",
				filesystemId, state.masterFor(filesystemId), state.myNodeId,
			)
			if e.Name != "moved" {
				return "", fmt.Errorf(
					"failed to move %s from %s to %s: %s",
					filesystemId, state.masterFor(filesystemId), state.myNodeId, e,
				)
			}
			// great - the current master thinks it's handed off to us.
			// doesn't mean we've actually mounted the filesystem yet
			// though, so wait on that here.

			state.filesystemsLock.Lock()
			if (*state.filesystems)[filesystemId].currentState == "active" {
				// great - we're already active
				log.Printf("Found %s was already active, giving it to Docker", filesystemId)
				state.filesystemsLock.Unlock()
			} else {
				for (*state.filesystems)[filesystemId].currentState != "active" {
					log.Printf(
						"%s was %s, waiting for it to change to active...",
						filesystemId, (*state.filesystems)[filesystemId].currentState,
					)
					// wait for state change
					stateChangeChan := make(chan interface{})
					(*state.filesystems)[filesystemId].transitionObserver.Subscribe(
						"transitions", stateChangeChan,
					)
					state.filesystemsLock.Unlock()
					_ = <-stateChangeChan
					state.filesystemsLock.Lock()
					(*state.filesystems)[filesystemId].transitionObserver.Unsubscribe(
						"transitions", stateChangeChan,
					)
				}
				log.Printf("%s finally changed to active, proceeding!", filesystemId)
				state.filesystemsLock.Unlock()
			}
		}
	} else {
		fsMachine, ch, err := state.CreateFilesystem(ctx, &name)
		if err != nil {
			return "", err
		}
		filesystemId = fsMachine.filesystemId
		if cloneName != "" {
			return "", fmt.Errorf("Cannot use branch-pinning syntax (docker run -v volume@branch:/path) to create a non-existent volume with a non-master branch")
		}
		log.Printf("WAITING FOR CREATE %s", name)
		e := <-ch
		if e.Name != "created" {
			return "", fmt.Errorf("Could not create volume %s: unexpected response %s - %s", name, e.Name, e.Args)
		}
		log.Printf("DONE CREATE %s", name)
	}
	return filesystemId, nil
}

func (state *InMemoryState) procureFilesystem(ctx context.Context, name VolumeName) (string, error) {
	var s string
	err := tryUntilSucceeds(func() error {
		ss, err := state.reallyProcureFilesystem(ctx, name)
		s = ss // bubble up
		return err
	}, "procuring filesystem")
	return s, err
}

func (s *InMemoryState) CreateFilesystem(
	ctx context.Context, filesystemName *VolumeName,
) (*fsMachine, chan *Event, error) {

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return nil, nil, err
	}

	// Check to see if it already partially exists, eg. in the registry but without a master
	var filesystemId string

	re, err := kapi.Get(
		context.Background(),
		fmt.Sprintf("%s/registry/filesystems/%s/%s", ETCD_PREFIX, filesystemName.Namespace, filesystemName.Name),
		&client.GetOptions{},
	)
	switch {
	case err != nil && !client.IsKeyNotFound(err):
		return nil, nil, err
	case err != nil && client.IsKeyNotFound(err):
		// Doesn't already exist, we can proceed as usual
		id, err := uuid.NewV4()
		if err != nil {
			return nil, nil, err
		}
		filesystemId = id.String()

		log.Printf("[CreateFilesystem] called with name=%+v, assigned id=%s", filesystemName, filesystemId)
		err = s.registry.RegisterFilesystem(ctx, *filesystemName, filesystemId)
		if err != nil {
			log.Printf(
				"[CreateFilesystem] Error while trying to register filesystem name %s => id %s: %s",
				filesystemName, filesystemId, err,
			)
			return nil, nil, err
		}
	// Proceed to set up master mapping
	default:
		// Key already exists
		var existingEntry registryFilesystem

		err := json.Unmarshal([]byte(re.Node.Value), &existingEntry)
		if err != nil {
			return nil, nil, err
		}

		filesystemId = existingEntry.Id
		log.Printf("[CreateFilesystem] called with name=%+v, examining existing id %s", filesystemName, filesystemId)

		// Check for an existing master mapping
		_, err = kapi.Get(
			context.Background(),
			fmt.Sprintf("%s/filesystems/masters/%s", ETCD_PREFIX, filesystemId),
			&client.GetOptions{},
		)
		if err != nil && !client.IsKeyNotFound(err) {
			return nil, nil, err
		} else if err != nil && client.IsKeyNotFound(err) {
			// Key not found, proceed to set up new master mapping
		} else {
			// Existing master mapping, we're trying to create an already-existing volume! Abort!
			return nil, nil, fmt.Errorf("A volume called %s already exists with id %s", filesystemName, filesystemId)
		}
	}

	if s.debugPartialFailCreateFilesystem {
		return nil, nil, fmt.Errorf("Injected fault for debugging/testing purposes")
	}

	// synchronize with etcd first, setting master to us only if the key
	// didn't previously exist, **before actually creating the filesystem**
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/filesystems/masters/%s", ETCD_PREFIX, filesystemId),
		s.myNodeId,
		&client.SetOptions{PrevExist: client.PrevNoExist},
	)
	if err != nil {
		log.Printf(
			"[CreateFilesystem] Error while trying to create key-that-does-not-exist in etcd prior to creating filesystem %s: %s",
			filesystemId, err,
		)
		return nil, nil, err
	}

	// update mastersCache with what we know. Do it in a func so that we
	// don't hold the mutex for longer than we need to.
	func() {
		s.mastersCacheLock.Lock()
		defer s.mastersCacheLock.Unlock()
		(*s.mastersCache)[filesystemId] = s.myNodeId
	}()

	// go ahead and create the filesystem
	fs := s.initFilesystemMachine(filesystemId)

	ch, err := s.dispatchEvent(filesystemId, &Event{Name: "create"}, "")
	if err != nil {
		log.Printf(
			"error during dispatch create! %s %s",
			filesystemId, err,
		)
		return nil, nil, err
	}

	return fs, ch, nil
}

// Returns a map from server name to a list of commit IDs that server is MISSING
func (s *InMemoryState) GetReplicationLatency(fs string) (error, map[string][]string) {
	commitsOnServer := map[string]map[string]struct{}{}
	allCommits := map[string]struct{}{}

	func() {
		s.globalSnapshotCacheLock.Lock()
		defer s.globalSnapshotCacheLock.Unlock()
		for server, filesystems := range *(s.globalSnapshotCache) {
			snapshots, ok := filesystems[fs]
			commitsOnServer[server] = map[string]struct{}{}
			if ok {
				commitsOnServer[server] = map[string]struct{}{}
				for _, snapshot := range snapshots {
					commitsOnServer[server][snapshot.Id] = struct{}{}
					allCommits[snapshot.Id] = struct{}{}
				}
			}
		}
	}()

	log.Printf("[GetReplicationLatency] got initial data: %+v", commitsOnServer)
	log.Printf("[GetReplicationLatency] all commits: %+v", allCommits)

	result := map[string][]string{}
	// Compute which elements are missing for each server
	for server, commits := range commitsOnServer {
		missingForServer := []string{}
		for commit, _ := range allCommits {
			_, ok := commits[commit]
			if !ok {
				missingForServer = append(missingForServer, commit)
			}
		}
		result[server] = missingForServer
	}

	log.Printf("[GetReplicationLatency] result: %+v", result)
	return nil, result
}
