package fsm

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/metrics"
	"github.com/dotmesh-io/dotmesh/pkg/observer"
	"github.com/dotmesh-io/dotmesh/pkg/registry"
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"
	"github.com/dotmesh-io/dotmesh/pkg/zfs"

	log "github.com/sirupsen/logrus"
)

type FsConfig struct {
	FilesystemID    string
	StateManager    StateManager
	Registry        registry.Registry
	UserManager     user.UserManager
	RegistryStore   store.RegistryStore
	FilesystemStore store.FilesystemStore
	ServerStore     store.ServerStore

	ContainerClient      container.Client
	LocalReceiveProgress observer.Observer
	NewSnapsOnMaster     observer.Observer
	DeathObserver        observer.Observer

	FilesystemMetadataTimeout int64

	// zfs executable path
	ZFSPath string
	// zpool executable path
	ZPoolPath string
	// Previously known as main.MOUNT_ZFS
	MountZFS string

	// PoolName is a required
	PoolName string
}

type FSM interface {
	ID() string
	Run()
	GetStatus() string
	GetCurrentState() string
	Mounted() bool

	Mount() (response *types.Event)
	Unmount() (response *types.Event)

	// TODO: review the call, maybe it's possible to internalize behaviour
	PushCompleted(success bool)
	// TODO: review the call, maybe it's possible to internalize behaviour
	PublishNewSnaps(server string, payload interface{}) error
	// TODO: review the call, maybe it's possible to internalize behaviour
	TransitionSubscribe(channel string, ch chan interface{})
	TransitionUnsubscribe(channel string, ch chan interface{})

	// metadata API
	GetMetadata(nodeID string) map[string]string
	ListMetadata() map[string]map[string]string
	SetMetadata(nodeID string, meta map[string]string)
	GetSnapshots(nodeID string) []*types.Snapshot
	ListSnapshots() map[string][]*types.Snapshot
	SetSnapshots(nodeID string, snapshots []*types.Snapshot)

	// Local snapshots from ZFS
	ListLocalSnapshots() []*types.Snapshot

	Submit(event *types.Event, requestID string) (reply chan *types.Event, err error)

	// WriteFile - reads the supplied Contents io.Reader and writes into the volume,
	// response will be sent to a provided Response channel
	WriteFile(source *types.InputFile)

	// ReadFile - reads a file from the volume into the supplied Contents io.Writer,
	// response will be sent to a provided Response channel
	ReadFile(destination *types.OutputFile)

	// DumpState is used for diagnostics
	DumpState() *FSMStateDump
}

// core functions used by files ending `state` which I couldn't think of a good place for.
func NewFilesystemMachine(cfg *FsConfig) *FsMachine {
	// initialize the FsMachine with a filesystem struct that has bare minimum
	// information (just the filesystem id) required to get started
	zfsInter, err := zfs.NewZFS(cfg.ZFSPath, cfg.ZPoolPath, cfg.PoolName, cfg.MountZFS)
	if err != nil {
		log.Fatalf("Failed initialising zfs interface, %s", err.Error())
	}
	return &FsMachine{
		filesystem: &types.Filesystem{
			Id: cfg.FilesystemID,
		},
		// stored here as well to avoid excessive locking on filesystem struct,
		// which gets clobbered, just to read its id
		filesystemId:            cfg.FilesystemID,
		requests:                make(chan *types.Event),
		innerRequests:           make(chan *types.Event),
		innerResponses:          make(chan *types.Event),
		fileInputIO:             make(chan *types.InputFile),
		fileOutputIO:            make(chan *types.OutputFile),
		responses:               map[string]chan *types.Event{},
		responsesLock:           &sync.Mutex{},
		snapshotsModified:       make(chan bool),
		containerClient:         cfg.ContainerClient,
		registryStore:           cfg.RegistryStore,
		serverStore:             cfg.ServerStore,
		filesystemStore:         cfg.FilesystemStore,
		state:                   cfg.StateManager,
		userManager:             cfg.UserManager,
		registry:                cfg.Registry,
		newSnapsOnMaster:        cfg.NewSnapsOnMaster,
		localReceiveProgress:    cfg.LocalReceiveProgress,
		snapshotsLock:           &sync.Mutex{},
		newSnapsOnServers:       observer.NewObserver(fmt.Sprintf("newSnapsOnServers:%s", cfg.FilesystemID)),
		currentState:            "discovering",
		status:                  "",
		lastTransitionTimestamp: time.Now().UnixNano(),
		transitionObserver:      observer.NewObserver(fmt.Sprintf("transitionObserver:%s", cfg.FilesystemID)),
		lastTransferRequest:     types.TransferRequest{},
		deathObserver:           cfg.DeathObserver,

		stateMachineMetadata:   make(map[string]map[string]string),
		stateMachineMetadataMu: &sync.RWMutex{},

		snapshotCache:   make(map[string][]*types.Snapshot),
		snapshotCacheMu: &sync.RWMutex{},
		// In the case where we're receiving a push (pushPeerState), it's the
		// POST handler on our http server which handles the receiving of the
		// snapshot. We need to coordinate with it so that we know when to
		// reload the list of snapshots, update etcd and coordinate our own
		// state changes, which we do via the POST handler sending on this
		// channel.
		pushCompleted:   make(chan bool),
		dirtyDelta:      0,
		sizeBytes:       0,
		transferUpdates: make(chan types.TransferUpdate),

		filesystemMetadataTimeout: cfg.FilesystemMetadataTimeout,
		zfs:                       zfsInter,
	}
}

func (f *FsMachine) ID() string {
	return f.filesystemId
}

func (f *FsMachine) GetCurrentState() string {
	return f.currentState
}

func (f *FsMachine) GetStatus() string {
	return f.status
}

func (f *FsMachine) TransitionSubscribe(channel string, ch chan interface{}) {
	f.transitionObserver.Subscribe(channel, ch)
}

func (f *FsMachine) TransitionUnsubscribe(channel string, ch chan interface{}) {
	f.transitionObserver.Unsubscribe(channel, ch)
}

func (f *FsMachine) PublishNewSnaps(server string, payload interface{}) error {
	return f.newSnapsOnServers.Publish(server, payload)
}

func (f *FsMachine) Mounted() bool {
	return f.filesystem.Mounted
}

func (f *FsMachine) Mount() (response *types.Event) {
	response, _ = f.mount()
	return
}

func (f *FsMachine) Unmount() (response *types.Event) {
	response, _ = f.unmount()
	return
}

func (f *FsMachine) PushCompleted(success bool) {
	f.pushCompleted <- success
}

type FSMStateDump struct {
	Filesystem              *types.Filesystem
	Status                  string
	CurrentState            string
	LastTransitionTimestamp int64

	LastTransferRequest   types.TransferRequest
	LastTransferRequestID string

	HandoffRequest *types.Event

	DirtyDelta int64
	SizeBytes  int64
}

// DumpState - dumps internal FsMachine state
// TODO: make copies instead of returning actual pointers
func (f *FsMachine) DumpState() *FSMStateDump {
	return &FSMStateDump{
		Filesystem:              f.filesystem,
		Status:                  f.status,
		CurrentState:            f.currentState,
		LastTransitionTimestamp: f.lastTransitionTimestamp,
		LastTransferRequest:     f.lastTransferRequest,
		LastTransferRequestID:   f.lastTransferRequestId,
		HandoffRequest:          f.handoffRequest,
		DirtyDelta:              f.dirtyDelta,
		SizeBytes:               f.sizeBytes,
	}
}

// Submit - submits event to a filesystem, returning the event stream for convenience so the caller
// can listen for a response
func (f *FsMachine) Submit(event *types.Event, requestID string) (reply chan *types.Event, err error) {
	if requestID == "" {
		requestID = uuid.New().String()
	}

	if event.Args == nil {
		event.Args = &types.EventArgs{}
	}
	(*event.Args)["RequestId"] = requestID
	f.responsesLock.Lock()
	defer f.responsesLock.Unlock()
	rc, ok := f.responses[requestID]
	if !ok {
		responseChan := make(chan *types.Event)
		f.responses[requestID] = responseChan
		rc = responseChan
	}

	// Now we have a response channel set up, it's safe to send the request.

	// Don't block the entire etcd event-loop just because one fsMachine isn't
	// ready to receive an event. Our response is a chan anyway, so consumers
	// can synchronize on reading from that as they wish (for example, in
	// another goroutine).
	go func() {
		f.requests <- event
	}()

	return rc, nil
}

func (f *FsMachine) Run() {
	// TODO cancel this when we eventually support deletion
	log.Printf("[run:%s] INIT", f.filesystemId)
	go f.runWhileFilesystemLives(
		f.markFilesystemAsLive,
		"markFilesystemAsLive",
		f.filesystemId,
		time.Duration(f.filesystemMetadataTimeout/2)*time.Second,
		time.Duration(f.filesystemMetadataTimeout/2)*time.Second,
	)
	// The success backoff time for updateEtcdAboutSnapshots is 0s
	// because it blocks on a channel anyway; inserting a success
	// backoff just means it'll be rate-limited as it'll sleep before
	// processing each snapshot!
	go f.runWhileFilesystemLives(
		f.updateEtcdAboutSnapshots,
		"updateEtcdAboutSnapshots",
		f.filesystemId,
		1*time.Second,
		0*time.Second,
	)
	go f.runWhileFilesystemLives(
		f.updateEtcdAboutTransfers,
		"updateEtcdAboutTransfers",
		f.filesystemId,
		1*time.Second,
		0*time.Second,
	)
	go f.runWhileFilesystemLives(
		f.pollDirty,
		"pollDirty",
		f.filesystemId,
		1*time.Second,
		1*time.Second,
	)

	go func() {

		for state := discoveringState; state != nil; {
			state = state(f)
		}

		f.transitionedTo("gone", "")

		// Senders close channels, receivers check for closedness.

		close(f.innerResponses)

		// TODO(karolis): check whether we really need to do this
		// as filesytem is deleted from the cache in state.DeleteFilesystem

		// Remove ourself from the filesystems map
		f.state.DeleteFilesystemFromMap(f.filesystemId)

		// Send a signal to anyone waiting for our death (see: InMemoryState.waitForFilesystemDeath)
		// This MUST happen AFTER the deletion from the filesystem map, to avoid a race in waitForFilesystemDeath
		f.terminateRunnersWhileFilesystemLived(f.filesystemId)

		log.Printf("[run:%s] terminated", f.filesystemId)
	}()

	// proxy requests and responses, enforcing an ordering, to avoid accepting
	// a new request before a response comes back, ie to serialize requests &
	// responses per-statemachine (without blocking the entire etcd event loop,
	// which asynchronously writes to the requests chan)
	for req := range f.requests {
		f.innerRequests <- req
		resp, more := <-f.innerResponses
		if !more {
			resp = &types.Event{
				Name: "filesystem-gone",
				Args: &types.EventArgs{},
			}
		}
		respChan, ok := func() (chan *types.Event, bool) {
			f.responsesLock.Lock()
			defer f.responsesLock.Unlock()
			respChan, ok := f.responses[(*req.Args)["RequestId"].(string)]
			return respChan, ok
		}()
		if ok {
			respChan <- resp
		} else {
			log.Warnf(
				"[run:%s] unable to find response chan '%s'! dropping resp %s :/",
				f.filesystemId,
				(*req.Args)["RequestId"].(string),
				resp,
			)
		}
	}
}

func (f *FsMachine) runWhileFilesystemLives(fn func() error, label string, filesystemId string, errorBackoff, successBackoff time.Duration) {
	deathChan := make(chan interface{})
	f.deathObserver.Subscribe(filesystemId, deathChan)
	defer f.deathObserver.Unsubscribe(filesystemId, deathChan)

	stillAlive := true
	for stillAlive {
		select {
		case <-deathChan:
			stillAlive = false
		default:
			err := fn()
			if err != nil {
				log.Printf(
					"Error in runWhileFilesystemLives(%s@%s), retrying in %s: %s",
					label, filesystemId, errorBackoff, err)
				time.Sleep(errorBackoff)
			} else {
				time.Sleep(successBackoff)
			}
		}
	}

}

func (f *FsMachine) terminateRunnersWhileFilesystemLived(filesystemId string) {
	f.deathObserver.Publish(filesystemId, struct{ reason string }{"runWhileFilesystemLives"})
}

func (f *FsMachine) pollDirty() error {

	if f.filesystem.Mounted {
		dirtyDelta, sizeBytes, err := f.zfs.GetDirtyDelta(f.filesystemId, f.latestSnapshot())
		if err != nil {
			return err
		}
		if f.dirtyDelta != dirtyDelta || f.sizeBytes != sizeBytes {
			f.dirtyDelta = dirtyDelta
			f.sizeBytes = sizeBytes

			fd := &types.FilesystemDirty{
				FilesystemID: f.filesystemId,
				NodeID:       f.state.NodeID(),
				DirtyBytes:   dirtyDelta,
				SizeBytes:    sizeBytes,
			}
			err = f.filesystemStore.SetDirty(fd, &store.SetOptions{})
			// _, err = f.etcdClient.Set(context.Background(), fmt.Sprintf("%s/filesystems/dirty/%s", types.EtcdPrefix, f.filesystemId), string(serialized), nil)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// return the latest snapshot id, or "" if none exists
func (f *FsMachine) latestSnapshot() string {
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	if len(f.filesystem.Snapshots) > 0 {
		return f.filesystem.Snapshots[len(f.filesystem.Snapshots)-1].Id
	}
	return ""
}

func (f *FsMachine) markFilesystemAsLive() error {
	return f.state.MarkFilesystemAsLiveInEtcd(f.filesystemId)
}

func (f *FsMachine) getCurrentPollResult() types.TransferPollResult {
	gr := make(chan types.TransferPollResult)
	f.transferUpdates <- types.TransferUpdate{
		Kind:      types.TransferGetCurrentPollResult,
		GetResult: gr,
	}
	return <-gr
}

func (f *FsMachine) updateEtcdAboutTransfers() error {
	pollResult := f.currentPollResult

	// wait until the state machine notifies us that it's changed the
	// transfer state, but have an escape clause in case this filesystem
	// is deleted so we don't block forever
	deathChan := make(chan interface{})
	f.deathObserver.Subscribe(f.filesystemId, deathChan)
	defer f.deathObserver.Unsubscribe(f.filesystemId, deathChan)

	for {
		var update types.TransferUpdate
		select {
		case update = <-(f.transferUpdates):
		case _ = <-deathChan:
			log.Infof("[updateEtcdAboutTransfers] Terminating due to filesystem death")
			return nil
		}

		switch update.Kind {
		case types.TransferStart:
			pollResult = update.Changes
		case types.TransferGotIds:
			pollResult.FilesystemId = update.Changes.FilesystemId
			pollResult.StartingCommit = update.Changes.StartingCommit
			pollResult.TargetCommit = update.Changes.TargetCommit
		case types.TransferCalculatedSize:
			pollResult.Status = update.Changes.Status
			pollResult.Size = update.Changes.Size
		case types.TransferTotalAndSize:
			pollResult.Status = update.Changes.Status
			pollResult.Total = update.Changes.Total
			pollResult.Size = update.Changes.Size
		case types.TransferProgress:
			// never update a transfer after it's finished
			if pollResult.Status != "finished" {
				pollResult.Sent = update.Changes.Sent
				if pollResult.Sent > pollResult.Size {
					// cap at 100%, so that all our clients don't have to
					pollResult.Sent = pollResult.Size
				}
				pollResult.NanosecondsElapsed = update.Changes.NanosecondsElapsed
				pollResult.Status = update.Changes.Status
			}
		case types.TransferIncrementIndex:
			if pollResult.Index < pollResult.Total {
				pollResult.Index++
			}
			pollResult.Sent += update.Changes.Size
		case types.TransferNextS3File:
			pollResult.Index += 1
			pollResult.Total += 1
			pollResult.Size = update.Changes.Size
			pollResult.Status = update.Changes.Status
		case types.TransferSent:
			pollResult.Sent = update.Changes.Sent
			pollResult.Status = update.Changes.Status
		case types.TransferFinished:
			pollResult.Status = "finished"
			pollResult.Index = pollResult.Total
		case types.TransferStatus:
			pollResult.Status = update.Changes.Status
		case types.TransferGetCurrentPollResult:
			update.GetResult <- pollResult
			continue
		default:
			return fmt.Errorf("Unknown transfer update kind in %#v", update)
		}

		// In all cases
		if update.Changes.Message != "" {
			pollResult.Message = update.Changes.Message
		}

		// Send the update
		// Immediately store the fact in our local state as well, to avoid
		// issues where we miss the "echo" from kv store when the update
		// happens locally.
		f.state.UpdateInterclusterTransfer(pollResult.TransferRequestId, pollResult)

		// And persist it to the kv store.
		err := f.filesystemStore.SetTransfer(&pollResult, &store.SetOptions{})
		if err != nil {
			return err
		}

		// Go around the loop for the next command
	}
}

func (f *FsMachine) updateEtcdAboutSnapshots() error {
	// as soon as we're connected, eagerly: if we know about some
	// snapshots, **or the absence of them**, set this in etcd.
	// serialized, err := func() ([]byte, error) {
	// 	f.snapshotsLock.Lock()
	// 	defer f.snapshotsLock.Unlock()
	// 	return json.Marshal(f.filesystem.Snapshots)
	// }()

	snaps := []*types.Snapshot{}
	f.snapshotsLock.Lock()
	for _, s := range f.filesystem.Snapshots {
		snaps = append(snaps, s.DeepCopy())
	}
	f.snapshotsLock.Unlock()

	// since we want atomic rewrites, we can just save the entire
	// snapshot data in a single key, as a json list. this is easier to
	// begin with! although we'll bump into the 1MB request limit in
	// etcd eventually.
	err := f.serverStore.SetSnapshots(&types.ServerSnapshots{
		ID:           f.state.NodeID(),
		FilesystemID: f.filesystemId,
		Snapshots:    snaps,
	})
	if err != nil {
		return err
	}

	// wait until the state machine notifies us that it's changed the
	// snapshots, but have an escape clause in case this filesystem is
	// deleted so we don't block forever
	deathChan := make(chan interface{})
	f.deathObserver.Subscribe(f.filesystemId, deathChan)
	defer f.deathObserver.Unsubscribe(f.filesystemId, deathChan)

	select {
	case <-f.snapshotsModified:
		log.Debugf("[updateEtcdAboutSnapshots] going 'round the loop")
	case <-deathChan:
		log.Infof("[updateEtcdAboutSnapshots] terminating due to filesystem death")
	}

	return nil
}

func (f *FsMachine) getCurrentState() string {
	// abusing snapshotsLock here, maybe we should have a separate lock over
	// these fields
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	return f.currentState
}

func (f *FsMachine) transitionedTo(state string, status string) {
	// abusing snapshotsLock here, maybe we should have a separate lock over
	// these fields
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	now := time.Now().UnixNano()
	log.Printf(
		"<transition> %s to %s %s (from %s %s, %.2fs ago)",
		f.filesystemId, state, status, f.currentState, f.status,
		float64(now-f.lastTransitionTimestamp)/float64(time.Second),
	)

	metrics.TransitionCounter.WithLabelValues(f.currentState, state, status).Add(1)

	f.currentState = state
	f.status = status
	f.lastTransitionTimestamp = now
	f.transitionObserver.Publish("transitions", state)

	update := map[string]string{
		"state": state, "status": status,
	}

	err := f.serverStore.SetState(&types.ServerState{
		ID:           f.state.NodeID(),
		FilesystemID: f.filesystemId,
		State:        update,
	})
	if err != nil {
		log.Printf("error updating KV store %+v: %+v", update, err)
		return
	}
	// fake an etcd version for anyone expecting a version field
	update["version"] = "0"

	// we don't hear our own echo, so set it locally too.
	f.stateMachineMetadataMu.Lock()
	_, ok := f.stateMachineMetadata[f.state.NodeID()]
	if !ok {
		f.stateMachineMetadata[f.state.NodeID()] = make(map[string]string)
	}
	f.stateMachineMetadata[f.state.NodeID()] = update
	f.stateMachineMetadataMu.Unlock()
}

func (f *FsMachine) fork(e *types.Event) (responseEvent *types.Event, nextState StateFn) {
	forkNamespaceIf, ok := (*e.Args)["ForkNamespace"]
	if !ok {
		return types.NewErrorEvent("cannot-fork", fmt.Errorf("namespace not specified")), activeState
	}
	forkNamespace, ok := forkNamespaceIf.(string)
	if !ok {
		return types.NewErrorEvent("cannot-fork", fmt.Errorf("type error: namespace is not a string")), activeState
	}

	forkNameIf, ok := (*e.Args)["ForkName"]
	if !ok {
		return types.NewErrorEvent("cannot-fork", fmt.Errorf("name not specified")), activeState
	}
	forkName, ok := forkNameIf.(string)
	if !ok {
		return types.NewErrorEvent("cannot-fork", fmt.Errorf("type error: name is not a string")), activeState
	}

	forkId := uuid.New().String()

	// Find our latest snapshot ID
	latestSnap := f.latestSnapshot()
	if latestSnap == "" {
		log.WithFields(log.Fields{
			"originFilesystemId": f.filesystemId,
			"originSnapshotId":   latestSnap,
			"forkNamespace":      forkNamespace,
			"forkName":           forkName,
			"forkId":             forkId,
			"local_snapshots":    f.ListLocalSnapshots(),
			"snapshots":          f.ListSnapshots(),
		}).Error("[fork] can't fork filesystem, can't detect snapshots")
		return types.NewErrorEvent("cannot-fork", fmt.Errorf("filesystem '%s' doesn't have any snapshots, cannot fork", f.ID())), activeState
	}

	log.WithFields(log.Fields{
		"originFilesystemId": f.filesystemId,
		"originSnapshotId":   latestSnap,
		"forkNamespace":      forkNamespace,
		"forkName":           forkName,
		"forkId":             forkId,
	}).Info("[fork] creating fork in zfs...")

	err := f.zfs.Fork(f.filesystemId, latestSnap, forkId)
	if err != nil {
		log.WithError(err).Error("Error generating fork")
		return types.NewErrorEvent("cannot-fork:error-generating-fork", err), activeState
	}

	// Register in registry
	err = f.state.RegisterNewFork(f.filesystemId, latestSnap, forkNamespace, forkName, forkId)
	if err != nil {
		log.WithError(err).Error("Error registering fork")
		return types.NewErrorEvent("cannot-fork:error-registering-fork", err), activeState
	}

	// go ahead and create the filesystem machine
	_, err = f.state.InitFilesystemMachine(forkId)
	if err != nil {
		return types.NewErrorEvent("cannot-fork:error-activating-statemachine", err), activeState
	}
	return &types.Event{Name: "forked", Args: &types.EventArgs{"ForkId": forkId}}, activeState
}

func (f *FsMachine) snapshot(e *types.Event) (responseEvent *types.Event, nextState StateFn) {
	var err error
	var meta map[string]string
	if val, ok := (*e.Args)["metadata"]; ok {
		meta, err = castToMetadata(val)
		if err != nil {
			log.WithFields(log.Fields{
				"error":          err,
				"filesystem_id":  f.ID(),
				"metadata_value": val,
			}).Error("[snapshot] failed to get metadata from event")
			return types.NewErrorEvent("unknown-metadata-format", err), backoffState
		}
	} else {
		meta = map[string]string{}
	}
	meta["timestamp"] = fmt.Sprintf("%d", time.Now().UnixNano())
	var snapshotId string
	snapshotIdInter, ok := (*e.Args)["snapshotId"]
	if !ok {
		snapshotId = uuid.New().String()
	} else {
		snapshotId = snapshotIdInter.(string)
	}
	metadataEncoded := encodeMapValues(meta)
	err = f.writeMetadata(metadataEncoded, f.filesystemId, snapshotId)
	if err != nil {
		log.WithError(err).Error("Failed writing commit metadata to file!!!")
		return &types.Event{
			Name: "failed-writing-metadata", Args: &types.EventArgs{"err": err.Error()},
		}, backoffState
	}
	output, err := f.zfs.Snapshot(f.filesystemId, snapshotId, make([]string, 0))
	if err != nil {
		return &types.Event{
			Name: "failed-snapshot",
			Args: &types.EventArgs{"err": fmt.Sprintf("%v", err), "combined-output": string(output)},
		}, backoffState
	}

	f.snapshotsLock.Lock()
	f.filesystem.Snapshots = append(f.filesystem.Snapshots, &types.Snapshot{Id: snapshotId, Metadata: meta})
	f.snapshotsLock.Unlock()

	err = f.snapshotsChanged()
	if err != nil {
		log.Errorf("[snapshot] %v while trying to inform that snapshots changed %s", err, f.zfs.FQ(f.filesystemId))
		return &types.Event{
			Name: "failed-snapshot-changed",
			Args: &types.EventArgs{"err": fmt.Sprintf("%v", err)},
		}, backoffState
	}
	return &types.Event{Name: "snapshotted", Args: &types.EventArgs{"SnapshotId": snapshotId}}, activeState
}

// find the user-facing name of a given filesystem id. if we're a branch
// (clone), return the name of our parent filesystem.
func (f *FsMachine) name() (types.VolumeName, error) {
	tlf, _, err := f.registry.LookupFilesystemById(f.filesystemId)
	return tlf.MasterBranch.Name, err
}

func (f *FsMachine) containersRunning() ([]container.DockerContainer, error) {
	name, err := f.name()
	if err != nil {
		return []container.DockerContainer{}, err
	}
	return f.containerClient.Related(name.String())
}

func (f *FsMachine) stopContainers() error {
	name, err := f.name()
	if err != nil {
		return err
	}
	return f.containerClient.Stop(name.StringWithoutAdmin())
}

func (f *FsMachine) startContainers() error {
	name, err := f.name()
	if err != nil {
		return err
	}
	return f.containerClient.Start(name.StringWithoutAdmin())
}

// probably the wrong way to do it
func pointers(snapshots []types.Snapshot) []*types.Snapshot {
	newList := []*types.Snapshot{}
	for _, snap := range snapshots {
		s := &types.Snapshot{}
		*s = snap
		newList = append(newList, s)
	}
	return newList
}

func (f *FsMachine) plausibleSnapRange() (*snapshotRange, error) {
	// get all snapshots for the given filesystem on the current master, and
	// then start a pull if we need to
	snapshots, err := f.state.SnapshotsForCurrentMaster(f.filesystemId)
	if err != nil {
		return nil, err
	}

	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	snapRange, err := canApply(pointers(snapshots), f.filesystem.Snapshots)

	return snapRange, err
}

func (f *FsMachine) attemptReceive() bool {
	// Check whether there are any pull-able snaps of this filesystem on its
	// current master

	_, err := f.plausibleSnapRange()

	// The non-error case plus all error cases except the ones below
	// indicate that some substantial action (receive, clone-and-rollback,
	// etc) is possible in receivingState, in those cases let's go there and
	// make progress.
	if err != nil {
		switch err := err.(type) {
		case *ToSnapsUpToDate:
			// no action, we're up-to-date
			log.Printf("[attemptReceive:%s] We're up to date", f.filesystemId)
			return false
		case *NoFromSnaps:
			// no snaps; can't replicate yet
			log.Printf("[attemptReceive:%s] There are no snapshots to receive", f.filesystemId)
			return false
		case *ToSnapsDiverged:
			// detected divergence, attempt to recieve and resolve
			log.Printf("[attemptReceive:%s] Detected divergence, attempting to receive", f.filesystemId)
			return true
		default:
			// some other error
			log.Printf("[attemptReceive:%s] Error %+v, not attempting to receive", f.filesystemId, err)
			return false
		}
	} else {
		// non-error canApply implies clean fastforward apply is possible
		log.Printf("[attemptReceive:%s] Detected clean fastforward, attempting to receive", f.filesystemId)
		return true
	}
}

// either missing because you're about to be locally created or because the
// filesystem exists somewhere else in the cluster

func (f *FsMachine) discover() error {
	// discover system state synchronously
	filesystem, err := f.zfs.DiscoverSystem(f.filesystemId)
	if err != nil {
		return err
	}

	f.snapshotsLock.Lock()
	f.filesystem = filesystem
	f.snapshotsLock.Unlock()

	err = f.snapshotsChanged()
	if err != nil {
		return fmt.Errorf("Error updating snapshots from discover(): %s", err)
	} else {
		return nil
	}
}

func (f *FsMachine) snapshotsChanged() error {
	// quite probably we just learned about some snapshots we didn't know about
	// before
	f.snapshotsModified <- true
	// we won't hear an "echo" from etcd about our own snapshots, so
	// synchronously update our own "global" cache about them, too, notifying
	// any observers in the process.
	// XXX this _might_ break the fact that handoff doesn't check what snapshot
	// it's notified about.
	var snaps []*types.Snapshot
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()

	for _, s := range f.filesystem.Snapshots {
		log.WithField("snapshot_id", s.Id).Info("[fsm.snapshotsChanged] reading snapshot data from filesystem")
		newMeta, err := f.getMetadata(s)
		if err != nil {
			log.WithFields(log.Fields{
				"error":       err,
				"snapshot_id": s.Id,
			}).Error("snapshotsChanged: couldn't read snapshot metadata")
		} else {
			s.Metadata = newMeta
		}
		snaps = append(snaps, s.DeepCopy())
	}
	return f.state.UpdateSnapshotsFromKnownState(
		f.state.NodeID(), f.filesystemId, snaps,
	)
}

// Attempt to recover from a divergence by creating a new branch from the current position, and rolling the
// existing branch back to rollbackTo.
// TODO: create a new local clone (branch), then roll back to
// rollbackTo (except, you can't roll back a snapshot
// that a clone depends on without promoting the clone... hmmm)

// step 4: Make dotmesh aware of the new branch
// ...something something FsMachine something etcd...

func (f *FsMachine) recoverFromDivergence(rollbackToId string) error {
	// Mint an ID for the new branch
	newFilesystemId := uuid.New().String()

	// Roll back the filesystem to rollbackTo, but leaving the new filesystem pointing to its original state
	err := f.zfs.StashBranch(f.filesystemId, newFilesystemId, rollbackToId)
	if err != nil {
		return err
	}

	tlf, parentBranchName, err := f.registry.LookupFilesystemById(f.filesystemId)
	if err != nil {
		return err
	}

	topLevelFilesystemId := tlf.MasterBranch.Id
	t := time.Now().UTC()
	newBranchName := ""
	if parentBranchName == "" {
		newBranchName = fmt.Sprintf("master-DIVERGED-%s", strings.Replace(t.Format(time.RFC3339), ":", "-", -1))
	} else {
		newBranchName = fmt.Sprintf("%s-DIVERGED-%s", parentBranchName, strings.Replace(t.Format(time.RFC3339), ":", "-", -1))
	}

	errorName, err := f.state.ActivateClone(topLevelFilesystemId, f.filesystemId, rollbackToId, newFilesystemId, newBranchName)

	if err != nil {
		return fmt.Errorf("Error recovering from divergence: %+v in %s", err, errorName)
	}

	return nil
}

// TODO this method shouldn't really be on a FsMachine, because it is
// parameterized by filesystemId (implicitly in pollResult, which varies over
// phases of a multi-filesystem push)

// TODO: spin up _three_ single node clusters, use one as a hub so that alice
// and bob can collaborate.

// TODO: run dind/dind-cluster.sh up, and then test the manifests in
// kubernetes/ against the resulting (3 node by default) cluster. Ensure things
// run offline. Figure out how to configure each cluster node with its own
// zpool. Test dynamic provisioning, and so on.
