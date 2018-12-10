package fsm

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"

	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/observer"
	"github.com/dotmesh-io/dotmesh/pkg/registry"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

type FsConfig struct {
	FilesystemID         string
	StateManager         StateManager
	Registry             registry.Registry
	UserManager          user.UserManager
	EtcdClient           client.KeysAPI
	ContainerClient      container.Client
	LocalReceiveProgress observer.Observer
	NewSnapsOnMaster     observer.Observer

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
	Run()
	GetStatus() string
	GetCurrentState() string
	Mounted() bool
	TransitionSubscribe(channel string, ch chan interface{})
	TransitionUnsubscribe(channel string, ch chan interface{})

	// metadata API
	GetMetadata(nodeID string) map[string]string
	ListMetadata() map[string]map[string]string
	SetMetadata(nodeID string, meta map[string]string)
	GetSnapshots(nodeID string) []*types.Snapshot
	ListSnapshots() map[string][]*types.Snapshot
	SetSnapshots(nodeID string, snapshots []*types.Snapshot)

	Submit(event *types.Event, requestID string) (reply chan *types.Event, err error)
}

// core functions used by files ending `state` which I couldn't think of a good place for.
func NewFilesystemMachine(cfg *FsConfig) *FsMachine {
	// initialize the FsMachine with a filesystem struct that has bare minimum
	// information (just the filesystem id) required to get started
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
		etcdClient:              cfg.EtcdClient,
		state:                   cfg.StateManager,
		userManager:             cfg.UserManager,
		registry:                cfg.Registry,
		newSnapsOnMaster:        cfg.LocalReceiveProgress,
		localReceiveProgress:    cfg.LocalReceiveProgress,
		snapshotsLock:           &sync.Mutex{},
		newSnapsOnServers:       observer.NewObserver(fmt.Sprintf("newSnapsOnServers:%s", cfg.FilesystemID)),
		currentState:            "discovering",
		status:                  "",
		lastTransitionTimestamp: time.Now().UnixNano(),
		transitionObserver:      observer.NewObserver(fmt.Sprintf("transitionObserver:%s", cfg.FilesystemID)),
		lastTransferRequest:     types.TransferRequest{},

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
		zfsPath:                   cfg.ZFSPath,
		mountZFS:                  cfg.MountZFS,
		zpoolPath:                 cfg.ZPoolPath,
		poolName:                  cfg.PoolName,
	}
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

func (f *FsMachine) Mounted() bool {
	return f.filesystem.Mounted
}

// Submit - submits event to a filesystem, returning the event stream for convenience so the caller
// can listen for a response
func (f *FsMachine) Submit(event *types.Event, requestID string) (reply chan *types.Event, err error) {
	// f.requests <- event
	if requestID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		requestID = id.String()
	}
	// fs, err := s.InitFilesystemMachine(filesystem)
	// if err != nil {
	// 	return nil, err
	// }
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
	go runWhileFilesystemLives(
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
	go runWhileFilesystemLives(
		f.updateEtcdAboutSnapshots,
		"updateEtcdAboutSnapshots",
		f.filesystemId,
		1*time.Second,
		0*time.Second,
	)
	go runWhileFilesystemLives(
		f.updateEtcdAboutTransfers,
		"updateEtcdAboutTransfers",
		f.filesystemId,
		1*time.Second,
		0*time.Second,
	)
	go runWhileFilesystemLives(
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
		// f.state.filesystemsLock.Lock()
		// defer f.state.filesystemsLock.Unlock()
		// We must hold the fslock while calling terminateRunners... to avoid a deadlock with
		// waitForFilesystemDeath in utils.go
		terminateRunnersWhileFilesystemLived(f.filesystemId)
		// delete(f.state.filesystems, f.filesystemId)

		f.state.DeleteFilesystemFromMap(f.filesystemId)

		log.Printf("[run:%s] terminated", f.filesystemId)
	}()
	// proxy requests and responses, enforcing an ordering, to avoid accepting
	// a new request before a response comes back, ie to serialize requests &
	// responses per-statemachine (without blocking the entire etcd event loop,
	// which asynchronously writes to the requests chan)
	log.Printf("[run:%s] reading from external requests", f.filesystemId)
	for req := range f.requests {
		log.Printf("[run:%s] got req: %s", f.filesystemId, req)
		log.Printf("[run:%s] writing to internal requests", f.filesystemId)
		f.innerRequests <- req
		log.Printf("[run:%s] reading from internal responses", f.filesystemId)
		resp, more := <-f.innerResponses
		if !more {
			log.Printf("[run:%s] statemachine is finished", f.filesystemId)
			resp = &types.Event{"filesystem-gone", &types.EventArgs{}}
		}
		log.Printf("[run:%s] got resp: %s", f.filesystemId, resp)
		log.Printf("[run:%s] writing to external responses", f.filesystemId)
		respChan, ok := func() (chan *types.Event, bool) {
			f.responsesLock.Lock()
			defer f.responsesLock.Unlock()
			respChan, ok := f.responses[(*req.Args)["RequestId"].(string)]
			return respChan, ok
		}()
		if ok {
			respChan <- resp
		} else {
			log.Printf(
				"[run:%s] unable to find response chan '%s'! dropping resp %s :/",
				f.filesystemId,
				(*req.Args)["RequestId"].(string),
				resp,
			)
		}
		log.Printf("[run:%s] reading from external requests", f.filesystemId)
	}
}

func (f *FsMachine) pollDirty() error {

	if f.filesystem.Mounted {
		dirtyDelta, sizeBytes, err := getDirtyDelta(f.zfsPath, f.poolName, f.filesystemId, f.latestSnapshot())
		if err != nil {
			return err
		}
		if f.dirtyDelta != dirtyDelta || f.sizeBytes != sizeBytes {
			f.dirtyDelta = dirtyDelta
			f.sizeBytes = sizeBytes

			serialized, err := json.Marshal(dirtyInfo{
				Server:     f.state.NodeID(),
				DirtyBytes: dirtyDelta,
				SizeBytes:  sizeBytes,
			})
			if err != nil {
				return err
			}

			_, err = f.etcdClient.Set(context.Background(), fmt.Sprintf("%s/filesystems/dirty/%s", types.EtcdPrefix, f.filesystemId), string(serialized), nil)
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

func (f *FsMachine) getResponseChan(reqId string, e *types.Event) (chan *types.Event, error) {
	f.responsesLock.Lock()
	defer f.responsesLock.Unlock()
	respChan, ok := f.responses[reqId]
	if !ok {
		return nil, fmt.Errorf("No such request id response channel %s", reqId)
	}
	return respChan, nil
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
	// attempt to connect to etcd
	// kapi, err := getEtcdKeysApi()
	// if err != nil {
	// return err
	// }

	pollResult := &(f.currentPollResult)

	// wait until the state machine notifies us that it's changed the
	// transfer state, but have an escape clause in case this filesystem
	// is deleted so we don't block forever
	deathChan := make(chan interface{})
	deathObserver.Subscribe(f.filesystemId, deathChan)
	defer deathObserver.Unsubscribe(f.filesystemId, deathChan)

	for {
		var update types.TransferUpdate
		select {
		case update = <-(f.transferUpdates):
			log.Debugf("[updateEtcdAboutTransfers] Received command %#v", update)
		case _ = <-deathChan:
			log.Infof("[updateEtcdAboutTransfers] Terminating due to filesystem death")
			return nil
		}

		switch update.Kind {
		case types.TransferStart:
			(*pollResult) = update.Changes
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
			pollResult.Sent = update.Changes.Sent
			pollResult.NanosecondsElapsed = update.Changes.NanosecondsElapsed
			pollResult.Status = update.Changes.Status
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
			update.GetResult <- *pollResult
			continue
		default:
			return fmt.Errorf("Unknown transfer update kind in %#v", update)
		}

		// In all cases
		if update.Changes.Message != "" {
			pollResult.Message = update.Changes.Message
		}

		// Send the update
		log.Debugf("[updateEtcdAboutTransfers] pollResult = %#v", *pollResult)

		serialized, err := json.Marshal(*pollResult)
		if err != nil {
			return err
		}
		_, err = f.etcdClient.Set(context.Background(), fmt.Sprintf("%s/filesystems/transfers/%s", types.EtcdPrefix, pollResult.TransferRequestId), string(serialized), nil)
		if err != nil {
			return err
		}

		// Go around the loop for the next command
	}
}

func (f *FsMachine) updateEtcdAboutSnapshots() error {
	// as soon as we're connected, eagerly: if we know about some
	// snapshots, **or the absence of them**, set this in etcd.
	serialized, err := func() ([]byte, error) {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		return json.Marshal(f.filesystem.Snapshots)
	}()

	// since we want atomic rewrites, we can just save the entire
	// snapshot data in a single key, as a json list. this is easier to
	// begin with! although we'll bump into the 1MB request limit in
	// etcd eventually.
	_, err = f.etcdClient.Set(
		context.Background(),
		fmt.Sprintf(
			"%s/servers/snapshots/%s/%s", types.EtcdPrefix,
			f.state.NodeID(), f.filesystemId,
		),
		string(serialized),
		nil,
	)
	if err != nil {
		return err
	}
	log.Debugf(
		"[updateEtcdAboutSnapshots] successfully set new snaps for %s on %s",
		f.filesystemId, f.state.NodeID(),
	)

	// wait until the state machine notifies us that it's changed the
	// snapshots, but have an escape clause in case this filesystem is
	// deleted so we don't block forever
	deathChan := make(chan interface{})
	deathObserver.Subscribe(f.filesystemId, deathChan)
	defer deathObserver.Unsubscribe(f.filesystemId, deathChan)

	select {
	case _ = <-f.snapshotsModified:
		log.Debugf("[updateEtcdAboutSnapshots] going 'round the loop")
	case _ = <-deathChan:
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

	transitionCounter.WithLabelValues(f.currentState, state, status).Add(1)

	f.currentState = state
	f.status = status
	f.lastTransitionTimestamp = now
	f.transitionObserver.Publish("transitions", state)

	// update etcd
	// kapi, err := getEtcdKeysApi()
	// if err != nil {
	// 	log.Printf("error connecting to etcd while trying to update states: %s", err)
	// 	return
	// }
	update := map[string]string{
		"state": state, "status": status,
	}
	serialized, err := json.Marshal(update)
	if err != nil {
		log.Printf("cannot serialize %s: %s", update, err)
		return
	}
	_, err = f.etcdClient.Set(
		context.Background(),
		// .../:server/:filesystem = {"state": "inactive", "status": "pulling..."}
		fmt.Sprintf(
			"%s/servers/states/%s/%s",
			types.EtcdPrefix, f.state.NodeID(), f.filesystemId,
		),
		string(serialized),
		nil,
	)
	if err != nil {
		log.Printf("error updating etcd %+v: %+v", update, err)
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

func (f *FsMachine) snapshot(e *types.Event) (responseEvent *types.Event, nextState StateFn) {
	var meta types.Metadata
	if val, ok := (*e.Args)["metadata"]; ok {
		meta = castToMetadata(val)
	} else {
		meta = types.Metadata{}
	}
	meta["timestamp"] = fmt.Sprintf("%d", time.Now().UnixNano())
	metadataEncoded, err := encodeMetadata(meta)
	if err != nil {
		return &types.Event{
			Name: "failed-metadata-encode", Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	var snapshotId string
	snapshotIdInter, ok := (*e.Args)["snapshotId"]
	if !ok {
		id, err := uuid.NewV4()
		if err != nil {
			return &types.Event{
				Name: "failed-uuid", Args: &types.EventArgs{"err": err},
			}, backoffState
		}
		snapshotId = id.String()
	} else {
		snapshotId = snapshotIdInter.(string)
	}
	args := []string{"snapshot"}
	args = append(args, metadataEncoded...)
	args = append(args, fq(f.poolName, f.filesystemId)+"@"+snapshotId)
	logZFSCommand(f.filesystemId, fmt.Sprintf("%s %s", f.zfsPath, strings.Join(args, " ")))
	out, err := exec.Command(f.zfsPath, args...).CombinedOutput()
	log.Printf("[snapshot] Attempting: zfs %s", args)
	if err != nil {
		log.Printf("[snapshot] %v while trying to snapshot %s (%s)", err, fq(f.poolName, f.filesystemId), args)
		return &types.Event{
			Name: "failed-snapshot",
			Args: &types.EventArgs{"err": err, "combined-output": string(out)},
		}, backoffState
	}
	list, err := exec.Command(f.zfsPath, "list", fq(f.poolName, f.filesystemId)+"@"+snapshotId).CombinedOutput()
	if err != nil {
		log.Printf("[snapshot] %v while trying to list snapshot %s (%s)", err, fq(f.poolName, f.filesystemId), args)
		return &types.Event{
			Name: "failed-snapshot",
			Args: &types.EventArgs{"err": err, "combined-output": string(out)},
		}, backoffState
	}
	log.Printf("[snapshot] listed snapshot: '%q'", strconv.Quote(string(list)))
	func() {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		log.Printf("[snapshot] Succeeded snapshotting (out: '%s'), saving: %+v", out, &types.Snapshot{
			Id: snapshotId, Metadata: meta,
		})
		f.filesystem.Snapshots = append(f.filesystem.Snapshots,
			&types.Snapshot{Id: snapshotId, Metadata: meta})
	}()
	err = f.snapshotsChanged()
	if err != nil {
		log.Printf("[snapshot] %v while trying to inform that snapshots changed %s (%s)", err, fq(f.poolName, f.filesystemId), args)
		return &types.Event{
			Name: "failed-snapshot-changed",
			Args: &types.EventArgs{"err": err},
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
	filesystem, err := discoverSystem(f.zfsPath, f.poolName, f.filesystemId)
	if err != nil {
		return err
	}

	f.snapshotsLock.Lock()
	f.filesystem = filesystem
	f.snapshotsLock.Unlock()

	return f.snapshotsChanged()
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
	for _, s := range f.filesystem.Snapshots {
		snaps = append(snaps, s.DeepCopy())
	}
	f.snapshotsLock.Unlock()
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
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	newFilesystemId := id.String()

	// Roll back the filesystem to rollbackTo, but leaving the new filesystem pointing to its original state
	err = stashBranch(f.zfsPath, f.poolName, f.filesystemId, newFilesystemId, rollbackToId)
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

func calculateSendArgs(poolName, fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string) []string {

	// toFilesystemId
	// snapRange.toSnap.Id
	// snapRange.fromSnap == nil?  --> fromSnapshotId == ""?
	// snapRange.fromSnap.Id

	var sendArgs []string
	var fromSnap string
	if fromSnapshotId == "" {
		fromSnap = "START"
		if fromFilesystemId != "" { // XXX wtf
			// This is a clone-origin based send
			fromSnap = fmt.Sprintf(
				"%s@%s", fromFilesystemId, fromSnapshotId,
			)
		}
	} else {
		fromSnap = fromSnapshotId
	}
	if fromSnap == "START" {
		// -R sends interim snapshots as well
		sendArgs = []string{
			"-p", "-R", fq(poolName, toFilesystemId) + "@" + toSnapshotId,
		}
	} else {
		// in clone case, fromSnap must be fully qualified
		if strings.Contains(fromSnap, "@") {
			// send a clone, so make it fully qualified
			fromSnap = fq(poolName, fromSnap)
		}
		sendArgs = []string{
			"-p", "-I", fromSnap, fq(poolName, toFilesystemId) + "@" + toSnapshotId,
		}
	}
	return sendArgs
}

/*
		Discover total number of bytes in replication stream by asking nicely:

			luke@hostess:/foo$ sudo zfs send -nP pool/foo@now2
			full    pool/foo@now2   105050056
			size    105050056
			luke@hostess:/foo$ sudo zfs send -nP -I pool/foo@now pool/foo@now2
			incremental     now     pool/foo@now2   105044936
			size    105044936

	   -n

		   Do a dry-run ("No-op") send.  Do not generate any actual send
		   data.  This is useful in conjunction with the -v or -P flags to
		   determine what data will be sent.

	   -P

		   Print machine-parsable verbose information about the stream
		   package generated.
*/
func predictSize(zfsPath, poolName, fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string) (int64, error) {
	sendArgs := calculateSendArgs(poolName, fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId)
	predictArgs := []string{"send", "-nP"}
	predictArgs = append(predictArgs, sendArgs...)

	sizeCmd := exec.Command(zfsPath, predictArgs...)

	log.Printf("[predictSize] predict command: %#v", sizeCmd)

	out, err := sizeCmd.CombinedOutput()
	log.Printf("[predictSize] Output of predict command: %v", string(out))
	if err != nil {
		log.Printf("[predictSize] Got error on predict command: %v", err)
		return 0, err
	}
	shrap := strings.Split(string(out), "\n")
	if len(shrap) < 2 {
		return 0, fmt.Errorf("Not enough lines in output %v", string(out))
	}
	sizeLine := shrap[len(shrap)-2]
	shrap = strings.Fields(sizeLine)
	if len(shrap) < 2 {
		return 0, fmt.Errorf("Not enough fields in %v", sizeLine)
	}

	size, err := strconv.ParseInt(shrap[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
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
