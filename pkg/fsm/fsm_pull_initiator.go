package fsm

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/context"

	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
)

func pullInitiatorState(f *FsMachine) StateFn {
	f.transitionedTo("pullInitiatorState", "requesting")
	// this is a write state. refuse to act if containers are running

	// refuse to pull if we have any containers running
	// TODO stop any containers being started, somehow. (by acquiring a lock?)
	containers, err := f.containersRunning()
	if err != nil {
		log.Printf(
			"Can't pull into filesystem while we can't list whether containers are using it",
		)
		f.innerResponses <- &types.Event{
			Name: "error-listing-containers-during-pull",
			Args: &types.EventArgs{"err": err},
		}
		return backoffState
	}
	if len(containers) > 0 {
		log.Printf("Can't pull into filesystem while containers are using it")
		f.innerResponses <- &types.Event{
			Name: "cannot-pull-while-containers-running",
			Args: &types.EventArgs{"containers": containers},
		}
		return backoffState
	}

	transferRequest := f.lastTransferRequest
	transferRequestId := f.lastTransferRequestId

	// TODO dedupe what follows wrt pushInitiatorState!
	client := dmclient.NewJsonRpcClient(
		transferRequest.User,
		transferRequest.Peer,
		transferRequest.ApiKey,
		transferRequest.Port,
	)

	var path types.PathToTopLevelFilesystem
	// XXX Not propagating context here; not needed for auth, but would be nice
	// for inter-cluster opentracing.
	err = client.CallRemote(context.Background(),
		"DotmeshRPC.DeducePathToTopLevelFilesystem", map[string]interface{}{
			"RemoteNamespace":      transferRequest.RemoteNamespace,
			"RemoteFilesystemName": transferRequest.RemoteName,
			"RemoteCloneName":      transferRequest.RemoteBranchName,
		},
		&path,
	)
	if err != nil {
		f.innerResponses <- &types.Event{
			Name: "cant-rpc-deduce-path",
			Args: &types.EventArgs{"err": err},
		}
		return backoffState
	}

	// register a poll result object.
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStart,
		Changes: TransferPollResultFromTransferRequest(
			transferRequestId, transferRequest, f.state.NodeID(),
			1, 1+len(path.Clones), "syncing metadata",
		),
	}

	// iterate over the path, attempting to pull each clone in turn.
	responseEvent, nextState := f.applyPath(path, func(f *FsMachine,
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
		transferRequestId string,
		client *dmclient.JsonRpcClient, transferRequest *types.TransferRequest,
	) (*types.Event, StateFn) {
		return f.retryPull(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
			transferRequestId, client, transferRequest)
	}, transferRequestId, client, &transferRequest)

	f.innerResponses <- responseEvent
	return nextState
}

func (f *FsMachine) pull(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	snapRange *snapshotRange,
	transferRequest *types.TransferRequest,
	transferRequestId *string,
	client *dmclient.JsonRpcClient,
) (responseEvent *types.Event, nextState StateFn) {
	// IMPORTANT NOTE:

	// Avoid using f.filesystemId in this code path, unless you really
	// mean it.  If the user clones a branch, then pull() will be
	// called by applyPath to pull the master (and any intermediate
	// branches) before it gets called for the branch that THIS
	// fsMachine corresponds to. So we may well be operating on
	// filesystems that AREN'T f.filesystemId. Any assumption that the
	// filesystem being pulled here IS the one we're the fsmachine for
	// may fail in interesting cases.

	// TODO if we just created the filesystem, become the master for it. (or
	// maybe this belongs in the metadata prenegotiation phase)
	f.updateTransfer("calculating size", "")

	// XXX This shouldn't be deduced here _and_ passed in as an argument (which
	// is then thrown away), it just makes the code confusing.
	pr := f.getCurrentPollResult()
	toFilesystemId = pr.FilesystemId
	fromSnapshotId = pr.StartingCommit

	// 1. Do an RPC to estimate the send size and update pollResult
	// accordingly.
	var size int64
	err := client.CallRemote(context.Background(),
		"DotmeshRPC.PredictSize", map[string]interface{}{
			"FromFilesystemId": fromFilesystemId,
			"FromSnapshotId":   fromSnapshotId,
			"ToFilesystemId":   toFilesystemId,
			"ToSnapshotId":     toSnapshotId,
		},
		&size,
	)
	if err != nil {
		return &types.Event{
			Name: "error-rpc-predict-size",
			Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	log.Printf("[pull] size: %d", size)

	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferCalculatedSize,
		Changes: types.TransferPollResult{
			Status: "pulling",
			Size:   size,
		},
	}

	// 2. Perform GET, as receivingState does. Update as we go, similar to how
	// push does it.
	var url string
	if transferRequest.Port == 0 {
		url, err = dmclient.DeduceUrl(
			context.Background(),
			[]string{transferRequest.Peer},
			// pulls are between clusters, so use external address where
			// appropriate
			"external",
			transferRequest.User,
			transferRequest.ApiKey,
		)
		if err != nil {
			return &types.Event{
				Name: "push-initiator-cant-deduce-url",
				Args: &types.EventArgs{"err": err},
			}, backoffState
		}
	} else {
		url = fmt.Sprintf("http://%s:%d", transferRequest.Peer, transferRequest.Port)
	}

	url = fmt.Sprintf(
		"%s/filesystems/%s/%s/%s",
		url,
		toFilesystemId,
		fromSnapshotId,
		toSnapshotId,
	)
	log.Printf("Pulling from %s", url)
	req, err := http.NewRequest(
		"GET", url, nil,
	)
	req.SetBasicAuth(
		transferRequest.User,
		transferRequest.ApiKey,
	)
	getClient := new(http.Client)
	resp, err := getClient.Do(req)
	if err != nil {
		log.Printf("Attempting to pull %s got %s", toFilesystemId, err)
		return &types.Event{
			Name: "get-failed-pull",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}
	log.Printf(
		"Debug: curl -u admin:[pw] %s",
		url,
	)
	// TODO finish rewriting return values and update pollResult as the transfer happens...

	// LUKE: Is this wrong?

	// When we pull a branch and nothing already exists, we have the
	// branch fsmachine doing the "pull" for the master and then then
	// branch.  In this case, f.filesystemId is the branch fsid, but
	// we're actually pulling the master branch (toFilesystemId)... but
	// we're saving it under the branch's name in zfs? This might explain how we get the symptoms seen:

	// 1) Pulling node has the branch fsid in zfs, but not the master fsid.

	// 2) Pulling node is trying to mount the master fsid and failing.

	// cmd := exec.Command(ZFS, "recv", fq(f.filesystemId))
	finished := make(chan bool)

	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	defer pipeWriter.Close()
	go utils.Pipe(
		resp.Body, fmt.Sprintf("http response body for %s", toFilesystemId),
		pipeWriter, "stdin of zfs recv",
		finished,

		// Permanently empty cancellation channel, and noop cancellation callback
		make(chan *types.Event),
		func(e *types.Event, c chan *types.Event) {},

		func(bytes int64, t int64) {
			f.transferUpdates <- types.TransferUpdate{
				Kind: types.TransferProgress,
				Changes: types.TransferPollResult{
					Status:             "pulling",
					Sent:               bytes,
					NanosecondsElapsed: t,
				},
			}

			f.transitionedTo("pullInitiatorState",
				fmt.Sprintf(
					"transferred %.2fMiB in %.2fs (%.2fMiB/s)...",
					// bytes => mebibytes       nanoseconds => seconds
					float64(bytes)/(1024*1024), float64(t)/(1000*1000*1000),
					// mib/sec
					(float64(bytes)/(1024*1024))/(float64(t)/(1000*1000*1000)),
				),
			)

			// Reject any waiting requests, leaving them waiting for
			// (potentially) hours will just annoy callers and make RPCs time
			// out which makes everyone more sad than being told "try again
			// later" straight away.
			select {
			case _ = <-f.innerRequests:
				f.innerResponses <- &types.Event{
					Name: "busy-transferring",
					Args: &types.EventArgs{
						"currentState": "pullInitiatorState",
						"progress":     bytes,
					},
				}
			default:
				// Non-blocking read, carry on if there are no senders to
				// disappoint.
			}

		},
		"decompress",
	)

	log.Printf("[pull] about to start consuming prelude on %v", pipeReader)
	prelude, err := ConsumePrelude(pipeReader)
	if err != nil {
		_ = <-finished
		return &types.Event{
			Name: "consume-prelude-failed",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}
	log.Printf("[pull] Got prelude %v", prelude)
	err = f.zfs.Recv(pipeReader, toFilesystemId, nil)
	pipeReader.Close()
	pipeWriter.Close()
	f.transitionedTo("receiving", "finished zfs recv")
	_ = <-finished
	f.transitionedTo("receiving", "finished pipe")

	if err != nil {
		log.Printf(
			"Got error %s when running zfs recv for %s, check the logs for output that looks like it's from zfs",
			err, toFilesystemId,
		)
		return &types.Event{
			Name: "get-failed-pull",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}
	log.Printf("[pull] about to start applying prelude on %v", pipeReader)
	err = f.zfs.ApplyPrelude(prelude, toFilesystemId)
	if err != nil {
		return &types.Event{
			Name: "failed-applying-prelude",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}

	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStatus,
		Changes: types.TransferPollResult{
			Status:  "finished",
			Message: "",
		},
	}

	log.Printf("Successfully received %s => %s for %s", fromSnapshotId, toSnapshotId, toFilesystemId)
	return &types.Event{
		Name: "finished-pull",
	}, discoveringState
}

func (f *FsMachine) retryPull(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	transferRequestId string,
	client *dmclient.JsonRpcClient, transferRequest *types.TransferRequest,
) (*types.Event, StateFn) {
	// TODO refactor the following with respect to retryPush!

	// Let's go!
	var remoteSnaps []*types.Snapshot
	err := client.CallRemote(
		context.Background(),
		"DotmeshRPC.CommitsById",
		toFilesystemId,
		&remoteSnaps,
	)
	if err != nil {
		return &types.Event{
			Name: "failed-getting-snapshots", Args: &types.EventArgs{"err": err},
		}, backoffState
	}

	// Interpret empty toSnapshotId as "pull up to the latest snapshot" _on the
	// remote_
	if toSnapshotId == "" {
		if len(remoteSnaps) == 0 {
			return &types.Event{
				Name: "no-snapshots-of-remote-filesystem",
				Args: &types.EventArgs{"filesystemId": toFilesystemId},
			}, backoffState
		}
		toSnapshotId = remoteSnaps[len(remoteSnaps)-1].Id
	}
	log.Printf(
		"[retryPull] from (%s, %s) to (%s, %s)",
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
	)

	fsMachine, err := f.state.InitFilesystemMachine(toFilesystemId)
	if err != nil {
		return &types.Event{
			Name: "retry-pull-cant-find-filesystem-id",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}
	localSnaps := fsMachine.ListLocalSnapshots()

	// if we're given a target snapshot, restrict f.filesystem.snapshots to
	// that snapshot
	remoteSnaps, err = restrictSnapshots(remoteSnaps, toSnapshotId)
	if err != nil {
		return &types.Event{
			Name: "restrict-snapshots-error",
			Args: &types.EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}
	snapRange, err := canApply(remoteSnaps, localSnaps)
	if err != nil {
		switch typedErr := err.(type) {
		case *ToSnapsUpToDate:
			// no action, we're up-to-date for this filesystem
			f.updateTransfer("finished", "remote already up-to-date, nothing to do")
			return &types.Event{
				Name: "peer-up-to-date",
			}, backoffState
		case *ToSnapsAhead:
			if transferRequest.StashDivergence {
				f.updateTransfer("finished", "This remote is ahead of the cluster you are pulling from - nothing to do")
				// in this case, there are no further snaps to pull from the other side as our local version was ahead
				return &types.Event{
					Name: "peer-up-to-date",
				}, backoffState
			} else {
				return &types.Event{
					Name: "error-in-canapply-when-pulling", Args: &types.EventArgs{"err": err},
				}, backoffState
			}
		case *ToSnapsDiverged:
			if transferRequest.StashDivergence {
				fmt.Printf("[retryPull] hit divergence case, have permission to stash - will stash local changes")
				e := f.recoverFromDivergence(typedErr.latestCommonSnapshot.Id)
				if e != nil {
					return &types.Event{
						Name: "failed-stashing",
						Args: &types.EventArgs{"err": e},
					}, backoffState
				}
				localSnaps, err = restrictSnapshots(remoteSnaps, typedErr.latestCommonSnapshot.Id)
				snapRange, err = canApply(remoteSnaps, localSnaps)
				if err != nil {
					return &types.Event{
						Name: "snapshot-canapply-error-after-diverge",
						Args: &types.EventArgs{"err": err},
					}, backoffState
				}
			} else {
				return &types.Event{
					Name: "error-in-canapply-when-pulling", Args: &types.EventArgs{"err": err},
				}, backoffState
			}
		default:
			return &types.Event{
				Name: "error-in-canapply-when-pulling", Args: &types.EventArgs{"err": err},
			}, backoffState
		}

	}
	var fromSnap string
	// XXX dedupe this wrt calculateSendArgs/predictSize
	if snapRange.fromSnap == nil {
		fromSnap = "START"
		if fromFilesystemId != "" {
			// This is a receive from a clone origin
			fromSnap = fmt.Sprintf(
				"%s@%s", fromFilesystemId, fromSnapshotId,
			)
		}
	} else {
		fromSnap = snapRange.fromSnap.Id
	}

	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferGotIds,
		Changes: types.TransferPollResult{
			FilesystemId:   toFilesystemId,
			StartingCommit: fromSnap,
			TargetCommit:   snapRange.toSnap.Id,
		},
	}

	var retry int
	var responseEvent *types.Event
	var nextState StateFn
	for retry < 5 {
		// XXX XXX XXX REFACTOR (retryPush)
		responseEvent, nextState = f.pull(
			fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
			snapRange, transferRequest, &transferRequestId, client,
		)
		if responseEvent.Name == "finished-pull" || responseEvent.Name == "peer-up-to-date" {
			log.Printf("[actualPull] Successful pull!")
			return responseEvent, nextState
		}
		retry++
		f.updateTransfer(
			fmt.Sprintf("retry %d", retry),
			fmt.Sprintf("Attempting to pull %s got %s", f.filesystemId, responseEvent),
		)
		log.Printf(
			"[retry attempt %d] squashing and retrying in %ds because we "+
				"got a %s (which tried to put us into %+v)...",
			retry, retry, responseEvent, nextState,
		)
		time.Sleep(time.Duration(retry) * time.Second)
	}
	log.Printf(
		"[actualPull] Maximum retry attempts exceeded, "+
			"returning latest error: %s (to move into state %+v)",
		responseEvent, nextState,
	)
	return &types.Event{
		Name: "maximum-retry-attempts-exceeded", Args: &types.EventArgs{"responseEvent": responseEvent},
	}, backoffState
}
