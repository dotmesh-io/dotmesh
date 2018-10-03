package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
)

// core functions used by files ending `state` which I couldn't think of a good place for.

func newFilesystemMachine(filesystemId string, s *InMemoryState) *fsMachine {
	// initialize the fsMachine with a filesystem struct that has bare minimum
	// information (just the filesystem id) required to get started
	return &fsMachine{
		filesystem: &filesystem{
			id: filesystemId,
		},
		// stored here as well to avoid excessive locking on filesystem struct,
		// which gets clobbered, just to read its id
		filesystemId:            filesystemId,
		requests:                make(chan *Event),
		innerRequests:           make(chan *Event),
		innerResponses:          make(chan *Event),
		responses:               map[string]chan *Event{},
		responsesLock:           &sync.Mutex{},
		snapshotsModified:       make(chan bool),
		state:                   s,
		snapshotsLock:           &sync.Mutex{},
		newSnapsOnServers:       NewObserver(fmt.Sprintf("newSnapsOnServers:%s", filesystemId)),
		currentState:            "discovering",
		status:                  "",
		lastTransitionTimestamp: time.Now().UnixNano(),
		transitionObserver:      NewObserver(fmt.Sprintf("transitionObserver:%s", filesystemId)),
		lastTransferRequest:     types.TransferRequest{},
		// In the case where we're receiving a push (pushPeerState), it's the
		// POST handler on our http server which handles the receiving of the
		// snapshot. We need to coordinate with it so that we know when to
		// reload the list of snapshots, update etcd and coordinate our own
		// state changes, which we do via the POST handler sending on this
		// channel.
		pushCompleted:   make(chan bool),
		dirtyDelta:      0,
		sizeBytes:       0,
		transferUpdates: make(chan TransferUpdate),
	}
}

func activateClone(state *InMemoryState, topLevelFilesystemId, originFilesystemId, originSnapshotId, newCloneFilesystemId, newBranchName string) (string, error) {
	// RegisterClone(name string, topLevelFilesystemId string, clone Clone)
	err := state.registry.RegisterClone(
		newBranchName, topLevelFilesystemId,
		Clone{
			newCloneFilesystemId,
			Origin{
				originFilesystemId, originSnapshotId,
			},
		},
	)
	if err != nil {
		return "failed-clone-registration", err
	}

	// spin off a state machine
	state.initFilesystemMachine(newCloneFilesystemId)
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return "failed-get-etcd", err
	}
	// claim the clone as mine, so that it can be mounted here
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf(
			"%s/filesystems/masters/%s", ETCD_PREFIX, newCloneFilesystemId,
		),
		state.myNodeId,
		// only modify current master if this is a new filesystem id
		&client.SetOptions{PrevExist: client.PrevNoExist},
	)
	if err != nil {
		return "failed-make-cloner-master", err
	}

	return "", nil
}

func (f *fsMachine) run() {
	// TODO cancel this when we eventually support deletion
	log.Printf("[run:%s] INIT", f.filesystemId)
	go runWhileFilesystemLives(
		f.markFilesystemAsLive,
		"markFilesystemAsLive",
		f.filesystemId,
		time.Duration(f.state.config.FilesystemMetadataTimeout/2)*time.Second,
		time.Duration(f.state.config.FilesystemMetadataTimeout/2)*time.Second,
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

		// Remove ourself from the filesystems map
		f.state.filesystemsLock.Lock()
		defer f.state.filesystemsLock.Unlock()
		// We must hold the fslock while calling terminateRunners... to avoid a deadlock with
		// waitForFilesystemDeath in utils.go
		terminateRunnersWhileFilesystemLived(f.filesystemId)
		delete(f.state.filesystems, f.filesystemId)

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
			resp = &Event{"filesystem-gone", &EventArgs{}}
		}
		log.Printf("[run:%s] got resp: %s", f.filesystemId, resp)
		log.Printf("[run:%s] writing to external responses", f.filesystemId)
		respChan, ok := func() (chan *Event, bool) {
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

func (f *fsMachine) pollDirty() error {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}
	if f.filesystem.mounted {
		dirtyDelta, sizeBytes, err := getDirtyDelta(
			f.filesystemId, f.latestSnapshot(),
		)
		if err != nil {
			return err
		}
		if f.dirtyDelta != dirtyDelta || f.sizeBytes != sizeBytes {
			f.dirtyDelta = dirtyDelta
			f.sizeBytes = sizeBytes

			serialized, err := json.Marshal(dirtyInfo{
				Server:     f.state.myNodeId,
				DirtyBytes: dirtyDelta,
				SizeBytes:  sizeBytes,
			})
			if err != nil {
				return err
			}
			_, err = kapi.Set(
				context.Background(),
				fmt.Sprintf(
					"%s/filesystems/dirty/%s", ETCD_PREFIX, f.filesystemId,
				),
				string(serialized),
				nil,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// return the latest snapshot id, or "" if none exists
func (f *fsMachine) latestSnapshot() string {
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	if len(f.filesystem.snapshots) > 0 {
		return f.filesystem.snapshots[len(f.filesystem.snapshots)-1].Id
	}
	return ""
}

func (f *fsMachine) getResponseChan(reqId string, e *Event) (chan *Event, error) {
	f.responsesLock.Lock()
	defer f.responsesLock.Unlock()
	respChan, ok := f.responses[reqId]
	if !ok {
		return nil, fmt.Errorf("No such request id response channel %s", reqId)
	}
	return respChan, nil
}

func (f *fsMachine) markFilesystemAsLive() error {
	return f.state.markFilesystemAsLiveInEtcd(f.filesystemId)
}

func (f *fsMachine) getCurrentPollResult() TransferPollResult {
	gr := make(chan TransferPollResult)
	f.transferUpdates <- TransferUpdate{
		Kind:      TransferGetCurrentPollResult,
		GetResult: gr,
	}
	return <-gr
}

func (f *fsMachine) updateEtcdAboutTransfers() error {
	// attempt to connect to etcd
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	pollResult := &(f.currentPollResult)

	// wait until the state machine notifies us that it's changed the
	// transfer state, but have an escape clause in case this filesystem
	// is deleted so we don't block forever
	deathChan := make(chan interface{})
	deathObserver.Subscribe(f.filesystemId, deathChan)
	defer deathObserver.Unsubscribe(f.filesystemId, deathChan)

	for {
		var update TransferUpdate
		select {
		case update = <-(f.transferUpdates):
			log.Printf("[updateEtcdAboutTransfers] Received command %#v", update)
		case _ = <-deathChan:
			log.Printf("[updateEtcdAboutTransfers] Terminating due to filesystem death")
			return nil
		}

		switch update.Kind {
		case TransferStart:
			(*pollResult) = update.Changes
		case TransferGotIds:
			pollResult.FilesystemId = update.Changes.FilesystemId
			pollResult.StartingCommit = update.Changes.StartingCommit
			pollResult.TargetCommit = update.Changes.TargetCommit
		case TransferCalculatedSize:
			pollResult.Status = update.Changes.Status
			pollResult.Size = update.Changes.Size
		case TransferTotalAndSize:
			pollResult.Status = update.Changes.Status
			pollResult.Total = update.Changes.Total
			pollResult.Size = update.Changes.Size
		case TransferProgress:
			pollResult.Sent = update.Changes.Sent
			pollResult.NanosecondsElapsed = update.Changes.NanosecondsElapsed
			pollResult.Status = update.Changes.Status
		case TransferIncrementIndex:
			if pollResult.Index < pollResult.Total {
				pollResult.Index++
			}
			pollResult.Sent += update.Changes.Size
		case TransferNextS3File:
			pollResult.Index += 1
			pollResult.Total += 1
			pollResult.Size = update.Changes.Size
			pollResult.Status = update.Changes.Status
		case TransferSent:
			pollResult.Sent = update.Changes.Sent
			pollResult.Status = update.Changes.Status
		case TransferFinished:
			pollResult.Status = "finished"
			pollResult.Index = pollResult.Total
		case TransferStatus:
			pollResult.Status = update.Changes.Status
		case TransferGetCurrentPollResult:
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
		log.Printf("[updateEtcdAboutTransfers] pollResult = %#v", *pollResult)

		serialized, err := json.Marshal(*pollResult)
		if err != nil {
			return err
		}
		_, err = kapi.Set(
			context.Background(),
			fmt.Sprintf("%s/filesystems/transfers/%s", ETCD_PREFIX, pollResult.TransferRequestId),
			string(serialized),
			nil,
		)
		if err != nil {
			return err
		}

		// Go around the loop for the next command
	}
}

func (f *fsMachine) updateEtcdAboutSnapshots() error {
	// attempt to connect to etcd
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	// as soon as we're connected, eagerly: if we know about some
	// snapshots, **or the absence of them**, set this in etcd.
	serialized, err := func() ([]byte, error) {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		return json.Marshal(f.filesystem.snapshots)
	}()

	// since we want atomic rewrites, we can just save the entire
	// snapshot data in a single key, as a json list. this is easier to
	// begin with! although we'll bump into the 1MB request limit in
	// etcd eventually.
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf(
			"%s/servers/snapshots/%s/%s", ETCD_PREFIX,
			f.state.myNodeId, f.filesystemId,
		),
		string(serialized),
		nil,
	)
	if err != nil {
		return err
	}
	log.Printf(
		"[updateEtcdAboutSnapshots] successfully set new snaps for %s on %s",
		f.filesystemId, f.state.myNodeId,
	)

	// wait until the state machine notifies us that it's changed the
	// snapshots, but have an escape clause in case this filesystem is
	// deleted so we don't block forever
	deathChan := make(chan interface{})
	deathObserver.Subscribe(f.filesystemId, deathChan)
	defer deathObserver.Unsubscribe(f.filesystemId, deathChan)

	select {
	case _ = <-f.snapshotsModified:
		log.Printf("[updateEtcdAboutSnapshots] going 'round the loop")
	case _ = <-deathChan:
		log.Printf("[updateEtcdAboutSnapshots] terminating due to filesystem death")
	}

	return nil
}

func (f *fsMachine) getCurrentState() string {
	// abusing snapshotsLock here, maybe we should have a separate lock over
	// these fields
	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	return f.currentState
}

func (f *fsMachine) transitionedTo(state string, status string) {
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
	kapi, err := getEtcdKeysApi()
	if err != nil {
		log.Printf("error connecting to etcd while trying to update states: %s", err)
		return
	}
	update := map[string]string{
		"state": state, "status": status,
	}
	serialized, err := json.Marshal(update)
	if err != nil {
		log.Printf("cannot serialize %s: %s", update, err)
		return
	}
	_, err = kapi.Set(
		context.Background(),
		// .../:server/:filesystem = {"state": "inactive", "status": "pulling..."}
		fmt.Sprintf(
			"%s/servers/states/%s/%s",
			ETCD_PREFIX, f.state.myNodeId, f.filesystemId,
		),
		string(serialized),
		nil,
	)
	if err != nil {
		log.Printf("error updating etcd %+v: %+v", update, err)
		return
	}
	// we don't hear our own echo, so set it locally too.
	f.state.globalStateCacheLock.Lock()
	defer f.state.globalStateCacheLock.Unlock()
	// fake an etcd version for anyone expecting a version field
	update["version"] = "0"
	if _, ok := f.state.globalStateCache[f.state.myNodeId]; !ok {
		f.state.globalStateCache[f.state.myNodeId] = map[string]map[string]string{}
	}
	f.state.globalStateCache[f.state.myNodeId][f.filesystemId] = update
}

func (f *fsMachine) snapshot(e *Event) (responseEvent *Event, nextState stateFn) {
	var meta metadata
	if val, ok := (*e.Args)["metadata"]; ok {
		meta = castToMetadata(val)
	} else {
		meta = metadata{}
	}
	meta["timestamp"] = fmt.Sprintf("%d", time.Now().UnixNano())
	metadataEncoded, err := encodeMetadata(meta)
	if err != nil {
		return &Event{
			Name: "failed-metadata-encode", Args: &EventArgs{"err": err},
		}, backoffState
	}
	var snapshotId string
	snapshotIdInter, ok := (*e.Args)["snapshotId"]
	if !ok {
		id, err := uuid.NewV4()
		if err != nil {
			return &Event{
				Name: "failed-uuid", Args: &EventArgs{"err": err},
			}, backoffState
		}
		snapshotId = id.String()
	} else {
		snapshotId = snapshotIdInter.(string)
	}
	args := []string{"snapshot"}
	args = append(args, metadataEncoded...)
	args = append(args, fq(f.filesystemId)+"@"+snapshotId)
	logZFSCommand(f.filesystemId, fmt.Sprintf("%s %s", ZFS, strings.Join(args, " ")))
	out, err := exec.Command(ZFS, args...).CombinedOutput()
	log.Printf("[snapshot] Attempting: zfs %s", args)
	if err != nil {
		log.Printf("[snapshot] %v while trying to snapshot %s (%s)", err, fq(f.filesystemId), args)
		return &Event{
			Name: "failed-snapshot",
			Args: &EventArgs{"err": err, "combined-output": string(out)},
		}, backoffState
	}
	list, err := exec.Command(ZFS, "list", fq(f.filesystemId)+"@"+snapshotId).CombinedOutput()
	if err != nil {
		log.Printf("[snapshot] %v while trying to list snapshot %s (%s)", err, fq(f.filesystemId), args)
		return &Event{
			Name: "failed-snapshot",
			Args: &EventArgs{"err": err, "combined-output": string(out)},
		}, backoffState
	}
	log.Printf("[snapshot] listed snapshot: '%q'", strconv.Quote(string(list)))
	func() {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		log.Printf("[snapshot] Succeeded snapshotting (out: '%s'), saving: %+v", out, &snapshot{
			Id: snapshotId, Metadata: &meta,
		})
		f.filesystem.snapshots = append(f.filesystem.snapshots,
			&snapshot{Id: snapshotId, Metadata: &meta})
	}()
	err = f.snapshotsChanged()
	if err != nil {
		log.Printf("[snapshot] %v while trying to inform that snapshots changed %s (%s)", err, fq(f.filesystemId), args)
		return &Event{
			Name: "failed-snapshot-changed",
			Args: &EventArgs{"err": err},
		}, backoffState
	}
	return &Event{Name: "snapshotted", Args: &EventArgs{"SnapshotId": snapshotId}}, activeState
}

// find the user-facing name of a given filesystem id. if we're a branch
// (clone), return the name of our parent filesystem.
func (f *fsMachine) name() (VolumeName, error) {
	tlf, _, err := f.state.registry.LookupFilesystemById(f.filesystemId)
	return tlf.MasterBranch.Name, err
}

func (f *fsMachine) containersRunning() ([]DockerContainer, error) {
	f.state.containersLock.Lock()
	defer f.state.containersLock.Unlock()
	name, err := f.name()
	if err != nil {
		return []DockerContainer{}, err
	}
	return f.state.containers.Related(name.String())
}

func (f *fsMachine) stopContainers() error {
	f.state.containersLock.Lock()
	defer f.state.containersLock.Unlock()
	name, err := f.name()
	if err != nil {
		return err
	}
	return f.state.containers.Stop(name.StringWithoutAdmin())
}

func (f *fsMachine) startContainers() error {
	f.state.containersLock.Lock()
	defer f.state.containersLock.Unlock()
	name, err := f.name()
	if err != nil {
		return err
	}
	return f.state.containers.Start(name.StringWithoutAdmin())
}

// probably the wrong way to do it
func pointers(snapshots []snapshot) []*snapshot {
	newList := []*snapshot{}
	for _, snap := range snapshots {
		s := &snapshot{}
		*s = snap
		newList = append(newList, s)
	}
	return newList
}

func (f *fsMachine) plausibleSnapRange() (*snapshotRange, error) {
	// get all snapshots for the given filesystem on the current master, and
	// then start a pull if we need to
	snapshots, err := f.state.snapshotsForCurrentMaster(f.filesystemId)
	if err != nil {
		return nil, err
	}

	f.snapshotsLock.Lock()
	defer f.snapshotsLock.Unlock()
	snapRange, err := canApply(pointers(snapshots), f.filesystem.snapshots)

	return snapRange, err
}

func (f *fsMachine) attemptReceive() bool {
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

func (f *fsMachine) discover() error {
	// discover system state synchronously
	filesystem, err := discoverSystem(f.filesystemId)
	if err != nil {
		return err
	}
	func() {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		f.filesystem = filesystem
	}()
	return f.snapshotsChanged()
}

func (f *fsMachine) snapshotsChanged() error {
	// quite probably we just learned about some snapshots we didn't know about
	// before
	f.snapshotsModified <- true
	// we won't hear an "echo" from etcd about our own snapshots, so
	// synchronously update our own "global" cache about them, too, notifying
	// any observers in the process.
	// XXX this _might_ break the fact that handoff doesn't check what snapshot
	// it's notified about.
	var snaps []*snapshot
	func() {
		f.snapshotsLock.Lock()
		defer f.snapshotsLock.Unlock()
		snaps = f.filesystem.snapshots
	}()

	// []*snapshot => []snapshot, gah
	snapsAlternate := []snapshot{}
	for _, snap := range snaps {
		snapsAlternate = append(snapsAlternate, *snap)
	}
	return f.state.updateSnapshotsFromKnownState(
		f.state.myNodeId, f.filesystemId, &snapsAlternate,
	)
}

// Attempt to recover from a divergence by creating a new branch from the current position, and rolling the
// existing branch back to rollbackTo.
// TODO: create a new local clone (branch), then roll back to
// rollbackTo (except, you can't roll back a snapshot
// that a clone depends on without promoting the clone... hmmm)

// step 4: Make dotmesh aware of the new branch
// ...something something fsmachine something etcd...

func (f *fsMachine) recoverFromDivergence(rollbackToId string) error {
	// Mint an ID for the new branch
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	newFilesystemId := id.String()

	// Roll back the filesystem to rollbackTo, but leaving the new filesystem pointing to its original state
	err = stashBranch(f.filesystemId, newFilesystemId, rollbackToId)
	if err != nil {
		return err
	}

	tlf, parentBranchName, err := f.state.registry.LookupFilesystemById(f.filesystemId)
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

	errorName, err := activateClone(f.state, topLevelFilesystemId, f.filesystemId, rollbackToId, newFilesystemId, newBranchName)

	if err != nil {
		return fmt.Errorf("Error recovering from divergence: %+v in %s", err, errorName)
	}

	return nil
}

func calculateSendArgs(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
) []string {

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
			"-p", "-R", fq(toFilesystemId) + "@" + toSnapshotId,
		}
	} else {
		// in clone case, fromSnap must be fully qualified
		if strings.Contains(fromSnap, "@") {
			// send a clone, so make it fully qualified
			fromSnap = fq(fromSnap)
		}
		sendArgs = []string{
			"-p", "-I", fromSnap, fq(toFilesystemId) + "@" + toSnapshotId,
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
func predictSize(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
) (int64, error) {
	sendArgs := calculateSendArgs(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId)
	predictArgs := []string{"send", "-nP"}
	predictArgs = append(predictArgs, sendArgs...)

	sizeCmd := exec.Command(ZFS, predictArgs...)

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

// TODO this method shouldn't really be on a fsMachine, because it is
// parameterized by filesystemId (implicitly in pollResult, which varies over
// phases of a multi-filesystem push)

// TODO: spin up _three_ single node clusters, use one as a hub so that alice
// and bob can collaborate.

// TODO: run dind/dind-cluster.sh up, and then test the manifests in
// kubernetes/ against the resulting (3 node by default) cluster. Ensure things
// run offline. Figure out how to configure each cluster node with its own
// zpool. Test dynamic provisioning, and so on.
