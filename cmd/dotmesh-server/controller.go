package main

import (
	"encoding/base64"
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"

	"os"
	"sort"
	"strings"
	"sync"
	"time"

	// "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"

	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/fsm"
	"github.com/dotmesh-io/dotmesh/pkg/messaging"
	"github.com/dotmesh-io/dotmesh/pkg/notification"
	"github.com/dotmesh-io/dotmesh/pkg/observer"
	"github.com/dotmesh-io/dotmesh/pkg/registry"
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/dotmesh-io/dotmesh/pkg/zfs"

	log "github.com/sirupsen/logrus"
)

type InMemoryState struct {
	config          Config
	filesystems     map[string]fsm.FSM
	filesystemsLock *sync.RWMutex

	serverAddressesCache     map[string][]string
	serverAddressesCacheLock *sync.RWMutex

	globalContainerCache     map[string]containerInfo
	globalContainerCacheLock *sync.RWMutex

	messenger       messaging.Messenger
	messagingServer messaging.MessagingServer

	registryStore   store.RegistryStore
	filesystemStore store.FilesystemStore
	serverStore     store.ServerStore

	etcdWaitTimestamp          int64
	etcdWaitState              string
	etcdWaitTimestampLock      *sync.Mutex
	localReceiveProgress       observer.Observer
	newSnapsOnMaster           observer.Observer
	deathObserver              observer.Observer
	registry                   registry.Registry
	containers                 container.Client
	containersLock             *sync.RWMutex
	fetchRelatedContainersChan chan bool
	interclusterTransfers      map[string]TransferPollResult
	interclusterTransfersLock  *sync.RWMutex
	globalDirtyCacheLock       *sync.RWMutex
	globalDirtyCache           map[string]dirtyInfo
	userManager                user.UserManager
	publisher                  notification.Publisher

	debugPartialFailCreateFilesystem bool
	debugPartialFailDelete           bool
	versionInfo                      *VersionInfo
	zfs                              zfs.ZFS
}

// typically methods on the InMemoryState "god object"

func NewInMemoryState(config Config) *InMemoryState {
	dockerClient, err := container.New(&container.Options{
		ContainerMountPrefix:  CONTAINER_MOUNT_PREFIX,
		ContainerMountDirLock: &containerMountDirLock,
	})
	if err != nil {
		// why do we panic so much here?
		log.WithFields(log.Fields{
			"error":                  err,
			"container_mount_prefix": CONTAINER_MOUNT_PREFIX,
		}).Fatal("inMemoryState: failed to configure docker client")
		os.Exit(1)
	}

	zfsInterface, err := zfs.NewZFS(config.ZFSExecPath, config.ZPoolPath, POOL, config.PoolName)
	if err != nil {
		// CG added this one but not a fan of panicing rather than returning
		panic(err)
	}

	s := &InMemoryState{
		config:                   config,
		filesystems:              make(map[string]fsm.FSM),
		filesystemsLock:          &sync.RWMutex{},
		serverAddressesCache:     make(map[string][]string),
		serverAddressesCacheLock: &sync.RWMutex{},
		// global container state (what containers are running where), filesystemId -> containerInfo
		globalContainerCache:     make(map[string]containerInfo),
		globalContainerCacheLock: &sync.RWMutex{},

		filesystemStore: config.FilesystemStore,
		registryStore:   config.RegistryStore,
		serverStore:     config.ServerStore,

		etcdWaitTimestamp:     0,
		etcdWaitState:         "",
		etcdWaitTimestampLock: &sync.Mutex{},
		// a sort of global event bus for filesystems getting new snapshots on
		// their masters, keyed on filesystem name, which interested parties
		// such as slaves for that filesystem may subscribe to
		newSnapsOnMaster:     observer.NewObserver("newSnapsOnMaster"),
		localReceiveProgress: observer.NewObserver("localReceiveProgress"),
		deathObserver:        observer.NewObserver("deathObserver"),
		// containers that are running with dotmesh volumes by filesystem id
		containers:     dockerClient,
		containersLock: &sync.RWMutex{},
		// channel to send on to hint that a new container is using a dotmesh
		// volume
		fetchRelatedContainersChan: make(chan bool),
		// inter-cluster transfers are recorded here
		interclusterTransfers:     make(map[string]TransferPollResult),
		interclusterTransfersLock: &sync.RWMutex{},
		globalDirtyCacheLock:      &sync.RWMutex{},
		globalDirtyCache:          make(map[string]dirtyInfo),
		userManager:               config.UserManager,
		// publisher:                 ,
		versionInfo: &VersionInfo{InstalledVersion: serverVersion},
		zfs:         zfsInterface,
	}

	publisher := notification.New(context.Background())
	_, err = publisher.Configure(&notification.Config{Attempts: 5})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("inMemoryState: failed to configure notification publisher")
		os.Exit(1)
	}
	s.publisher = publisher
	// a registry of names of filesystems and branches (clones) mapping to
	// their ids
	s.registry = registry.NewRegistry(config.UserManager, config.RegistryStore)

	err = s.initializeMessaging()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("[NATS] inMemoryState: messaging setup failed")
		os.Exit(1)
	}

	return s
}

func (s *InMemoryState) subscribeToClusterEvents(ctx context.Context) error {
	ch, err := s.messenger.Subscribe(ctx, &types.SubscribeQuery{
		Type: types.EventTypeClusterRequest,
	})
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-ch:

			log.WithFields(log.Fields{
				"id":   event.ID,
				"type": event.Type,
				"name": event.Name,
			}).Info("[subscribeToClusterEvents] cluster event received")

			switch event.Name {
			case "reset-registry":
				s.resetRegistry()
				resp := types.NewEvent("reset-registry-complete")
				resp.Type = types.EventTypeClusterResponse
				resp.ID = event.ID
				err = s.messenger.Publish(resp)
				if err != nil {
					log.WithFields(log.Fields{
						"error":    err,
						"type":     event.Type,
						"event_id": event.ID,
					}).Error("[subscribeToClusterEvents] failed to send event response")
				}
			}
		}
	}
}

func (s *InMemoryState) resetRegistry() {
	s.registry = registry.NewRegistry(s.userManager, s.config.RegistryStore)
}

func (s *InMemoryState) getOne(ctx context.Context, fs string) (DotmeshVolume, error) {
	// TODO simplify this by refactoring it into multiple functions,
	// simplifying locking in the process.
	master, err := s.registry.CurrentMasterNode(fs)
	if err != nil {
		return DotmeshVolume{}, err
	}

	if tlf, clone, err := s.registry.LookupFilesystemById(fs); err == nil {
		authorized, err := s.config.UserManager.Authorize(auth.GetUserFromCtx(ctx), true, &tlf)

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
		s.globalDirtyCacheLock.RLock()

		dirty, ok := s.globalDirtyCache[fs]
		var dirtyBytes int64
		var sizeBytes int64
		if ok {
			dirtyBytes = dirty.DirtyBytes
			sizeBytes = dirty.SizeBytes
		}
		s.globalDirtyCacheLock.RUnlock()
		// if not exists, 0 is fine

		fsm, err := s.GetFilesystemMachine(fs)
		if err != nil {
			return DotmeshVolume{}, err
		}

		// s.globalSnapshotCacheLock.RLock()
		// snapshots, ok := s.globalSnapshotCache[master][fs]
		// var commitCount int64
		// if ok {
		commitCount := int64(len(fsm.GetSnapshots(master)))
		// }
		// s.globalSnapshotCacheLock.RUnlock()

		d := DotmeshVolume{
			Name:                 tlf.MasterBranch.Name,
			Branch:               clone,
			Master:               master,
			DirtyBytes:           dirtyBytes,
			SizeBytes:            sizeBytes,
			Id:                   fs,
			CommitCount:          commitCount,
			ServerStates:         map[string]string{},
			ServerStatuses:       map[string]string{},
			ForkParentId:         tlf.ForkParentId,
			ForkParentSnapshotId: tlf.ForkParentSnapshotId,
		}
		s.serverAddressesCacheLock.Lock()
		defer s.serverAddressesCacheLock.Unlock()

		servers := []Server{}
		for server, addresses := range s.serverAddressesCache {
			servers = append(servers, Server{
				Id: server, Addresses: addresses,
			})
		}
		sort.Sort(ByAddress(servers))

		for _, server := range servers {
			numSnapshots := len(fsm.GetSnapshots(server.Id))
			state := fsm.GetMetadata(server.Id)
			status := ""
			if len(state) == 0 {
				status = fmt.Sprintf("unknown, %d snaps", numSnapshots)
				d.ServerStates[server.Id] = "unknown"
			} else {
				status = fmt.Sprintf(
					"%s, %d commits",
					state["status"], numSnapshots,
				)
				d.ServerStates[server.Id] = state["state"]
			}
			d.ServerStatuses[server.Id] = status
		}

		return d, nil
	} else {
		return DotmeshVolume{}, fmt.Errorf("Unable to find filesystem name for id %s", fs)
	}
}

func (s *InMemoryState) subscribeToFilesystemRequests(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ch, err := s.messenger.Subscribe(ctx, &types.SubscribeQuery{
				Type: types.EventTypeRequest,
			})
			if err != nil {
				if err != nil {
					log.WithFields(log.Fields{
						"error": err,
						"host":  s.config.NatsConfig.Host,
						"port":  s.config.NatsConfig.Port,
					}).Error("[NATS] failed to subscribe to filesystem events, retrying...")
				}
				time.Sleep(1 * time.Second)
				continue
			}

			for req := range ch {
				go func(r *types.Event) {
					err := s.processFilesystemEvent(r)
					if err != nil {
						log.WithFields(log.Fields{
							"error":         err,
							"filesystem_id": r.FilesystemID,
							"request_id":    r.ID,
						}).Error("[NATS] failed to process filesystem event")
					}
				}(req)
			}
		}
	}
}

func (s *InMemoryState) processFilesystemEvent(event *types.Event) error {
	masterNode, ok := s.registry.GetMasterNode(event.FilesystemID)
	if !ok {
		return nil
	}
	if masterNode != s.NodeID() {
		return nil
	}

	c, err := s.dispatchEvent(event.FilesystemID, event, event.ID)
	if err != nil {
		return err
	}

	internalResponse := <-c

	return s.respondToEvent(event.FilesystemID, event.ID, internalResponse)
}

func (s *InMemoryState) notifyPushCompleted(filesystemId string, success bool) {

	f, err := s.GetFilesystemMachine(filesystemId)
	if err != nil {
		log.Printf("[notifyPushCompleted] No such filesystem id %s", filesystemId)
		return
	}
	log.Printf("[notifyPushCompleted:%s] about to notify chan with success=%t", filesystemId, success)
	f.PushCompleted(success)
	log.Printf("[notifyPushCompleted:%s] done notify chan", filesystemId)
}

func (s *InMemoryState) getCurrentState(filesystemId string) (string, error) {
	// init fsMachine in case it isn't.
	// XXX this trusts (authenticated) POST data :/
	fs, err := s.GetFilesystemMachine(filesystemId)
	if err != nil {
		return "", err
	}
	return fs.GetCurrentState(), nil
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

	return s.userManager.NewAdmin(&user.User{
		Id:       ADMIN_USER_UUID,
		Name:     "admin",
		Password: []byte(strings.TrimSpace(string(adminPassword))),
		ApiKey:   strings.TrimSpace(string(adminKey)),
	})
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
		<-s.fetchRelatedContainersChan
	}
}

func (s *InMemoryState) findRelatedContainers() error {
	s.containersLock.Lock()
	defer s.containersLock.Unlock()
	containerMap, err := s.containers.AllRelated()
	if err != nil {
		return err
	}
	// log.Printf("findRelatedContainers got containerMap %s", containerMap)
	// kapi, err := getEtcdKeysApi()
	// if err != nil {
	// 	return err
	// }

	// Iterate over _every_ filesystem id we know we are masters for on this
	// system, zeroing out the etcd record of containers running on that
	// filesystem unless we just learned about them. (This means that when a
	// container stops, it no longer shows as running.)

	myFilesystems := []string{}

	filesystems := s.registry.ListMasterNodes(&registry.ListMasterNodesQuery{NodeID: s.NodeID()})
	for fs := range filesystems {
		myFilesystems = append(myFilesystems, fs)
	}

	for _, filesystemId := range myFilesystems {
		// update etcd with the list of containers and this node; we'll learn
		// about the state via our own watch on etcd
		// (0)/(1)dotmesh.io/(2)filesystems/(3)containers/(4):filesystem_id =>
		// {"server": "server", "containers": [{Name: "name", ID: "id"}]}
		theContainers, ok := containerMap[filesystemId]
		var value types.FilesystemContainers
		if ok {
			value = types.FilesystemContainers{
				NodeID:       s.NodeID(),
				FilesystemID: filesystemId,
				Containers:   theContainers,
			}
		} else {
			value = types.FilesystemContainers{
				NodeID:       s.NodeID(),
				FilesystemID: filesystemId,
				Containers:   []container.DockerContainer{},
			}
		}

		// update our local globalContainerCache immediately, so that we reduce
		// the window for races against setting this cache value.
		s.globalContainerCacheLock.Lock()
		// TODO: replace globalContainerCache containerInfo type with types.FilesystemContainers
		s.globalContainerCache[filesystemId] = containerInfo{
			Server:     value.NodeID,
			Containers: value.Containers,
		}
		s.globalContainerCacheLock.Unlock()

		err = s.filesystemStore.SetContainers(&value, &store.SetOptions{})

		if err != nil {
			log.WithFields(log.Fields{
				"filesystem": filesystemId,
				"error":      err,
			}).Error("findRelatedContainers: failed to set related containers")
		}
	}
	return nil
}

func (state *InMemoryState) reallyProcureFilesystem(ctx context.Context, name VolumeName) (string, error) {
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

	filesystemId, err := state.registry.MaybeCloneFilesystemId(name, cloneName)
	if err == nil {
		// TODO can we synchronize with the state machine somehow, to
		// ensure that we're not currently on a master in the process of
		// doing a handoff?
		master, err := state.registry.CurrentMasterNode(filesystemId)
		if err != nil {
			return "", err
		}
		if master == state.zfs.GetPoolID() {
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
					Args: &EventArgs{"target": state.zfs.GetPoolID()},
				},
			)
			if err != nil {
				return "", err
			}

			var e *Event
			select {
			case <-time.After(30 * time.Second):
				// something needs to read the response from the
				// response chan
				go func() { <-responseChan }()
				// TODO implement some kind of liveness check to avoid
				// timing out too early on slow transfers.
				return "", fmt.Errorf(
					"timed out trying to procure %s, please try again", filesystemId,
				)
			case e = <-responseChan:
				// tally ho!
			}

			if e.Name != "moved" {
				return "", fmt.Errorf(
					"failed to move %s from %s to %s: %s",
					filesystemId, master, state.zfs.GetPoolID(), e,
				)
			}
			// great - the current master thinks it's handed off to us.
			// doesn't mean we've actually mounted the filesystem yet
			// though, so wait on that here.

			state.filesystemsLock.Lock()
			if state.filesystems[filesystemId].GetCurrentState() == "active" {
				// great - we're already active

				state.filesystemsLock.Unlock()
			} else {
				for state.filesystems[filesystemId].GetCurrentState() != "active" {

					// wait for state change
					stateChangeChan := make(chan interface{})
					state.filesystems[filesystemId].TransitionSubscribe("transitions", stateChangeChan)
					state.filesystemsLock.Unlock()
					<-stateChangeChan
					state.filesystemsLock.Lock()
					state.filesystems[filesystemId].TransitionUnsubscribe("transitions", stateChangeChan)
				}

				state.filesystemsLock.Unlock()
			}
		}
	} else {
		fsMachine, ch, err := state.CreateFilesystem(ctx, &name)
		if err != nil {
			return "", err
		}
		filesystemId = fsMachine.ID()
		if cloneName != "" {
			return "", fmt.Errorf("Cannot use branch-pinning syntax (docker run -v volume@branch:/path) to create a non-existent volume with a non-master branch")
		}

		e := <-ch
		if e.Name != "created" {
			return "", fmt.Errorf("Could not create volume %s: unexpected response %s - %s", name, e.Name, e.Args)
		}

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

func (s *InMemoryState) CreateFilesystem(ctx context.Context, filesystemName *VolumeName) (fsm.FSM, chan *Event, error) {

	// Check to see if it already partially exists, eg. in the registry but without a master
	var filesystemId string

	existingFS, err := s.registryStore.GetFilesystem(filesystemName.Namespace, filesystemName.Name)

	switch {
	case err != nil && !store.IsKeyNotFound(err):
		return nil, nil, err
	case err != nil && store.IsKeyNotFound(err):
		// Doesn't already exist, we can proceed as usual
		filesystemId = uuid.New().String()

		err = s.registry.RegisterFilesystem(ctx, *filesystemName, filesystemId)
		if err != nil {
			log.WithFields(log.Fields{
				"error":           err,
				"filesystem_id":   filesystemId,
				"filesystem_name": filesystemName,
			}).Error("[CreateFilesystem] Error while trying to register filesystem name)")
			return nil, nil, err
		}
	default:
		filesystemId = existingFS.Id

		// Check for an existing master mapping
		_, err = s.filesystemStore.GetMaster(filesystemId)
		if err != nil && !store.IsKeyNotFound(err) {
			return nil, nil, err
		} else if err != nil && err == store.ErrNotFound {
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
	err = s.filesystemStore.SetMaster(&types.FilesystemMaster{
		FilesystemID: filesystemId,
		NodeID:       s.NodeID(),
	}, &store.SetOptions{})
	if err != nil {
		log.Errorf(
			"[CreateFilesystem] Error while trying to create key-that-does-not-exist in etcd prior to creating filesystem %s: %s",
			filesystemId, err,
		)
		return nil, nil, err
	}

	// update mastersCache with what we know
	s.registry.SetMasterNode(filesystemId, s.NodeID())

	// go ahead and create the filesystem
	fs, err := s.InitFilesystemMachine(filesystemId)
	if err != nil {
		return nil, nil, err
	}

	ch, err := s.dispatchEvent(filesystemId, &Event{Name: "create"}, "")
	if err != nil {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": filesystemId,
		}).Error("error during dispatch create")
		return nil, nil, err
	}

	return fs, ch, nil
}

// Returns a map from server name to a list of commit IDs that server is MISSING
func (s *InMemoryState) GetReplicationLatency(fs string) map[string][]string {
	commitsOnServer := map[string]map[string]struct{}{}
	allCommits := map[string]struct{}{}
	result := map[string][]string{}

	fsm, err := s.GetFilesystemMachine(fs)
	if err != nil {
		log.Errorf("[GetReplicationLatency] failed to get filesystem: %s", err)
		return result
	}

	serversAndSnapshots := fsm.ListSnapshots()

	for server, snapshots := range serversAndSnapshots {
		commitsOnServer[server] = map[string]struct{}{}

		for _, snapshot := range snapshots {
			commitsOnServer[server][snapshot.Id] = struct{}{}
			allCommits[snapshot.Id] = struct{}{}
		}
	}

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

	return result
}

// Volumes might be dots or branches, we get 'em all in one big list
func (s *InMemoryState) GetListOfVolumes(ctx context.Context) ([]DotmeshVolume, error) {
	result := []DotmeshVolume{}

	filesystems := s.registry.FilesystemIdsIncludingClones()

	for _, fs := range filesystems {
		one, err := s.getOne(ctx, fs)
		// Just skip this in the result list if the context (eg authenticated
		// user) doesn't have permission to read it.
		if err != nil {
			switch err := err.(type) {
			case PermissionDenied:
				continue
			default:
				log.Errorf("[GetListOfVolumes] err: %v", err)
				// If we got an error looking something up, it might just be
				// because the fsMachine list or the registry is temporarily
				// inconsistent wrt the mastersCache. Proceed, at the risk of
				// lying slightly...
				continue
			}
		}

		result = append(result, one)
	}

	return result, nil
}

func (state *InMemoryState) mustCleanupSocket() {
	if _, err := os.Stat(PLUGINS_DIR); err != nil {
		if err := os.MkdirAll(PLUGINS_DIR, 0700); err != nil {
			log.Fatalf("Could not make plugin directory %s: %v", PLUGINS_DIR, err)
		}
	}
	if _, err := os.Stat(DM_SOCKET); err == nil {
		if err = os.Remove(DM_SOCKET); err != nil {
			log.Fatalf("Could not clean up existing socket at %s: %v", DM_SOCKET, err)
		}
	}
}

func (state *InMemoryState) initFilesystemMachines() {

	for _, filesystemId := range state.zfs.FindFilesystemIdsOnSystem() {
		go func(fsID string) {
			_, err := state.InitFilesystemMachine(fsID)
			if err != nil {
				log.WithFields(log.Fields{
					"error":         err,
					"filesystem_id": fsID,
				}).Warn("[main] failed to initialize filesystem machine during boot")
			}
		}(filesystemId)
	}
}
