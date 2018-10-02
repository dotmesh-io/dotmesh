package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/net/context"

	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func pushInitiatorState(f *fsMachine) stateFn {
	// Deduce the latest snapshot in
	// f.lastTransferRequest.LocalFilesystemName:LocalCloneName
	// and try a few times to get it onto the target node.
	f.transitionedTo("pushInitiatorState", "requesting")
	// Set /filesystems/transfers/:transferId = TransferPollResult{...}
	transferRequest := f.lastTransferRequest
	transferRequestId := f.lastTransferRequestId
	log.Printf(
		"[pushInitiator] request: %v %+v",
		transferRequestId,
		transferRequest,
	)
	path, err := f.state.registry.DeducePathToTopLevelFilesystem(
		VolumeName{transferRequest.LocalNamespace, transferRequest.LocalName},
		transferRequest.LocalBranchName,
	)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cant-calculate-path-to-snapshot",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}

	pollResult := TransferPollResultFromTransferRequest(
		transferRequestId, transferRequest, f.state.myNodeId,
		1, 1+len(path.Clones), "syncing metadata",
	)
	f.lastPollResult = &pollResult

	err = updatePollResult(transferRequestId, pollResult)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "push-initiator-cant-write-to-etcd",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	// Also RPC to remote cluster to set up a similar record there.
	// TODO retries
	client := dmclient.NewJsonRpcClient(
		transferRequest.User,
		transferRequest.Peer,
		transferRequest.ApiKey,
		transferRequest.Port,
	)

	// TODO should we wait for the remote to ack that it's gone into the right state?

	// retryPush takes filesystem id to push, and final snapshot id (or ""
	// for "up to latest")

	// TODO tidy up argument passing here.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	responseEvent, nextState := f.applyPath(path, func(f *fsMachine,
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
		transferRequestId string, pollResult *TransferPollResult,
		client *dmclient.JsonRpcClient, transferRequest *types.TransferRequest,
	) (*Event, stateFn) {
		return f.retryPush(
			fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
			transferRequestId, pollResult, client, transferRequest, ctx,
		)
	}, transferRequestId, &pollResult, client, &transferRequest)

	f.innerResponses <- responseEvent
	if nextState == nil {
		panic("nextState != nil invariant failed")
	}
	return nextState
}

func (f *fsMachine) push(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	snapRange *snapshotRange,
	transferRequest *types.TransferRequest,
	transferRequestId *string,
	pollResult *TransferPollResult,
	client *dmclient.JsonRpcClient,
	ctx context.Context,
) (responseEvent *Event, nextState stateFn) {

	filesystemId := pollResult.FilesystemId

	// XXX This shouldn't be deduced here _and_ passed in as an argument (which
	// is then thrown away), it just makes the code confusing.
	fromSnapshotId = pollResult.StartingCommit

	pollResult.Status = "calculating size"
	err := updatePollResult(*transferRequestId, *pollResult)
	if err != nil {
		return &Event{
			Name: "push-initiator-cant-write-to-etcd",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	postReader, postWriter := io.Pipe()

	defer postWriter.Close()
	defer postReader.Close()

	var url string
	if transferRequest.Port == 0 {
		url, err = dmclient.DeduceUrl(
			ctx,
			[]string{transferRequest.Peer},
			// pushes are between clusters, so use external address where
			// appropriate
			"external",
			transferRequest.User,
			transferRequest.ApiKey,
		)
		if err != nil {
			return &Event{
				Name: "push-initiator-cant-deduce-url",
				Args: &EventArgs{"err": err},
			}, backoffState
		}
	} else {
		url = fmt.Sprintf("http://%s:%d", transferRequest.Peer, transferRequest.Port)
	}
	url = fmt.Sprintf(
		"%s/filesystems/%s/%s/%s",
		url,
		filesystemId,
		fromSnapshotId,
		snapRange.toSnap.Id,
	)
	log.Printf("Pushing to %s", url)
	req, err := http.NewRequest(
		"POST", url,
		postReader,
	)
	if err != nil {
		log.Printf("Attempting to push %s got %s", filesystemId, err)
		return &Event{
			Name: "error-starting-post-when-pushing",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	// TODO remove duplication (with replication.go)
	// command writes into pipe
	var cmd *exec.Cmd
	// https://github.com/zfsonlinux/zfs/pull/5189
	//
	// Due to the above issues, -R doesn't send user properties on
	// platforms we care about (notably, the version of ZFS that is bundled
	// with Ubuntu 16.04 and 16.10).
	//
	// Workaround this limitation by include the missing information in
	// JSON format in a "prelude" section of the ZFS send stream.
	//
	prelude, err := f.state.calculatePrelude(toFilesystemId, toSnapshotId)
	if err != nil {
		return &Event{
			Name: "error-calculating-prelude",
			Args: &EventArgs{"err": err, "filesystemId": toFilesystemId},
		}, backoffState
	}

	// TODO test whether toFilesystemId and toSnapshotId are set correctly,
	// and consistently with snapRange?
	sendArgs := calculateSendArgs(
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
	)
	realArgs := []string{"send"}
	realArgs = append(realArgs, sendArgs...)

	// XXX this doesn't need to happen every push(), just once above.
	size, err := predictSize(
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
	)
	if err != nil {
		return &Event{
			Name: "error-predicting",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	log.Printf("[actualPush:%s] size: %d", filesystemId, size)
	pollResult.Size = size
	pollResult.Status = "pushing"
	err = updatePollResult(*transferRequestId, *pollResult)
	if err != nil {
		return &Event{
			Name: "push-initiator-cant-write-to-etcd",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	// proceed to do real send
	logZFSCommand(filesystemId, fmt.Sprintf("%s %s", ZFS, strings.Join(realArgs, " ")))
	cmd = exec.Command(ZFS, realArgs...)
	pipeReader, pipeWriter := io.Pipe()

	defer pipeWriter.Close()
	defer pipeReader.Close()

	// we will write this to the pipe first, in the goroutine which writes
	preludeEncoded, err := encodePrelude(prelude)
	if err != nil {
		return &Event{
			Name: "cant-encode-prelude",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	cmd.Stdout = pipeWriter
	cmd.Stderr = getLogfile("zfs-send-errors")

	finished := make(chan bool)
	go pipe(
		pipeReader, fmt.Sprintf("stdout of zfs send for %s", filesystemId),
		postWriter, "http request body",
		finished,
		make(chan *Event),
		func(e *Event, c chan *Event) {},
		func(bytes int64, t int64) {
			pollResult.Sent = bytes
			pollResult.NanosecondsElapsed = t
			err = updatePollResult(*transferRequestId, *pollResult)
			if err != nil {
				log.Printf("Error updating poll result: %s", err)
			}
			f.transitionedTo("pushInitiatorState",
				fmt.Sprintf(
					"transferred %.2fMiB in %.2fs (%.2fMiB/s)...",
					// bytes => mebibytes       nanoseconds => seconds
					float64(bytes)/(1024*1024), float64(t)/(1000*1000*1000),
					// mib/sec
					(float64(bytes)/(1024*1024))/(float64(t)/(1000*1000*1000)),
				),
			)
		},
		"compress",
	)

	req.SetBasicAuth(
		transferRequest.User,
		transferRequest.ApiKey,
	)
	postClient := new(http.Client)

	log.Printf("[actualPush:%s] About to postClient.Do with req %+v", filesystemId, req)

	// postClient.Do will block trying to read the first byte of the request
	// body. But, we won't be able to provide the first byte until we start
	// running the command. So, do what we always do to avoid a deadlock. Run
	// something in a goroutine. In this case we need 'resp' in scope, so let's
	// run the command in a goroutine.

	errch := make(chan error)
	go func() {
		// This goroutine does all the writing to the HTTP POST
		log.Printf(
			"[actualPush:%s] Writing prelude of %d bytes (encoded): %s",
			filesystemId,
			len(preludeEncoded), preludeEncoded,
		)
		_, err = pipeWriter.Write(preludeEncoded)
		if err != nil {
			log.Printf("[actualPush:%s] Error writing prelude: %+v (sent to errch)", filesystemId, err)
			errch <- err
			log.Printf("[actualPush:%s] errch accepted prelude error, woohoo", filesystemId)
		}

		log.Printf(
			"[actualPush:%s] About to Run() for %s => %s",
			filesystemId, fromSnapshotId, toSnapshotId,
		)

		runErr := cmd.Run()

		log.Printf(
			"[actualPush:%s] Run() got result %s, about to put it into errch after closing pipeWriter",
			filesystemId,
			runErr,
		)
		err := pipeWriter.Close()
		if err != nil {
			log.Printf("[actualPush:%s] error closing pipeWriter: %s", filesystemId, err)
		}
		log.Printf(
			"[actualPush:%s] Writing to errch: %+v",
			filesystemId,
			runErr,
		)
		errch <- runErr
		log.Printf("[actualPush:%s] errch accepted it, woohoo", filesystemId)
	}()

	resp, err := postClient.Do(req)
	if err != nil {
		log.Printf("[actualPush:%s] error in postClient.Do: %s", filesystemId, err)

		go func() {
			_ = <-errch
		}()
		_ = <-finished
		return &Event{
			Name: "error-from-post-when-pushing",
			Args: &EventArgs{"err": err},
		}, backoffState
	}
	defer resp.Body.Close()
	log.Printf("[actualPush:%s] started HTTP request", filesystemId)

	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf(
			"[actualPush:%s] Got error while reading response body %s: %s",
			filesystemId,
			string(responseBody), err,
		)

		go func() {
			_ = <-errch
		}()
		_ = <-finished
		return &Event{
			Name: "error-reading-push-response-body",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	log.Printf("[actualPush:%s] Got response body while pushing: status %d, body %s", filesystemId, resp.StatusCode, string(responseBody))

	if resp.StatusCode != 200 {
		go func() {
			_ = <-errch
		}()
		_ = <-finished
		return &Event{
			Name: "error-pushing-posting",
			Args: &EventArgs{
				"requestURL":      url,
				"responseBody":    string(responseBody),
				"statusCode":      fmt.Sprintf("%d", resp.StatusCode),
				"responseHeaders": fmt.Sprintf("%+v", resp.Header),
			},
		}, backoffState
	}

	log.Printf("[actualPush:%s] Waiting for finish signal...", filesystemId)
	_ = <-finished
	log.Printf("[actualPush:%s] Done!", filesystemId)

	log.Printf("[actualPush:%s] reading from errch", filesystemId)
	err = <-errch
	log.Printf(
		"[actualPush:%s] Finished Run() for %s => %s: %s",
		filesystemId, fromSnapshotId, toSnapshotId, err,
	)
	if err != nil {
		log.Printf(
			"[actualPush:%s] Error from zfs send from %s => %s: %s, check zfs-send-errors.log",
			filesystemId, fromSnapshotId, toSnapshotId, err,
		)
		return &Event{
			Name: "error-from-writing-prelude-and-zfs-send",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	// XXX Adding the log messages below seemed to stop a deadlock, not sure
	// why. For now, let's just leave them in...
	// XXX what about closing post{Writer,Reader}?
	log.Printf("[actualPush:%s] Closing pipes...", filesystemId)
	pipeWriter.Close()
	pipeReader.Close()

	pollResult.Status = "finished"
	err = updatePollResult(*transferRequestId, *pollResult)
	if err != nil {
		return &Event{
			Name: "error-updating-poll-result",
			Args: &EventArgs{"err": err},
		}, backoffState
	}

	// TODO update the transfer record, release the peer state machines
	return &Event{
		Name: "finished-push",
		Args: &EventArgs{},
	}, discoveringState
}

func stash(filesystemId, snapId string, client *dmclient.JsonRpcClient, ctx context.Context) (*Event, stateFn) {
	var newBranch string
	e := client.CallRemote(
		ctx,
		"DotmeshRPC.StashAfter",
		types.StashRequest{
			FilesystemId: filesystemId,
			SnapshotId:   snapId,
		},
		&newBranch,
	)
	if e != nil {
		return &Event{
			Name: "failed-stashing-remote-end", Args: &EventArgs{"err": e},
		}, backoffState
	}
	return nil, discoveringState
}

func (f *fsMachine) retryPush(
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	transferRequestId string, pollResult *TransferPollResult,
	client *dmclient.JsonRpcClient, transferRequest *types.TransferRequest, ctx context.Context,
) (*Event, stateFn) {
	// Let's go!
	var retry int
	var responseEvent *Event
	nextState := backoffState

	for retry < 5 {
		select {
		case <-ctx.Done():
			break
		default:
		}
		// TODO refactor this wrt retryPull
		responseEvent, nextState = func() (*Event, stateFn) {
			// Interpret empty toSnapshotId as "push to the latest snapshot"
			if toSnapshotId == "" {
				snaps, err := f.state.snapshotsForCurrentMaster(toFilesystemId)
				if err != nil {
					return &Event{
						Name: "failed-getting-local-snapshots", Args: &EventArgs{"err": err},
					}, backoffState
				}
				if len(snaps) == 0 {
					return &Event{
						Name: "no-snapshots-of-that-filesystem",
						Args: &EventArgs{"filesystemId": toFilesystemId},
					}, backoffState
				}
				toSnapshotId = snaps[len(snaps)-1].Id
			}
			log.Printf(
				"[retryPush] from (%s, %s) to (%s, %s), pollResult: %+v",
				fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId, pollResult,
			)
			var remoteSnaps []*snapshot
			err := client.CallRemote(
				ctx,
				"DotmeshRPC.CommitsById",
				toFilesystemId,
				&remoteSnaps,
			)
			if err != nil {
				return &Event{
					Name: "failed-getting-remote-snapshots", Args: &EventArgs{"err": err},
				}, backoffState
			}
			fsMachine, err := f.state.maybeFilesystem(toFilesystemId)
			if err != nil {
				return &Event{
					Name: "retry-push-cant-find-filesystem-id",
					Args: &EventArgs{"err": err, "filesystemId": toFilesystemId},
				}, backoffState
			}
			var snaps []*snapshot
			func() {
				fsMachine.snapshotsLock.Lock()
				defer fsMachine.snapshotsLock.Unlock()
				snaps = fsMachine.filesystem.snapshots
			}()
			// if we're given a target snapshot, restrict f.filesystem.snapshots to
			// that snapshot
			localSnaps, err := restrictSnapshots(snaps, toSnapshotId)
			if err != nil {
				return &Event{
					Name: "restrict-snapshots-error",
					Args: &EventArgs{"err": err, "filesystemId": toFilesystemId},
				}, backoffState
			}
			snapRange, err := canApply(localSnaps, remoteSnaps)
			if err != nil {
				switch err := err.(type) {
				case *ToSnapsUpToDate:
					// no action, we're up-to-date for this filesystem
					pollResult.Status = "finished"
					pollResult.Message = "remote already up-to-date, nothing to do"

					e := updatePollResult(transferRequestId, *pollResult)
					if e != nil {
						return &Event{
							Name: "push-initiator-cant-write-to-etcd", Args: &EventArgs{"err": e},
						}, backoffState
					}
					return &Event{
						Name: "peer-up-to-date",
					}, backoffState
				// here we tell the other end to get it's house in order, then return an error so we go round the loop again to get the commit list etc.
				case *ToSnapsDiverged:
					if transferRequest.StashDivergence {
						event, state := stash(toFilesystemId, err.latestCommonSnapshot.Id, client, ctx)
						if event != nil {
							return event, state
						}
					}
				case *ToSnapsAhead:
					if transferRequest.StashDivergence {
						event, state := stash(toFilesystemId, err.latestCommonSnapshot.Id, client, ctx)
						if event != nil {
							return event, state
						}
					}
				}
				return &Event{
					Name: "error-in-canapply-when-pushing", Args: &EventArgs{"err": err},
				}, backoffState

			}
			// TODO peer may error out of pushPeerState, wouldn't we like to get them
			// back into it somehow? we could attempt to do that with by sending a new
			// RegisterTransfer rpc if necessary. or they could retry also.

			var fromSnap string
			if snapRange.fromSnap == nil {
				fromSnap = "START"
				if fromFilesystemId != "" {
					// This is a send from a clone origin
					fromSnap = fmt.Sprintf(
						"%s@%s", fromFilesystemId, fromSnapshotId,
					)
				}
			} else {
				fromSnap = snapRange.fromSnap.Id
			}

			pollResult.FilesystemId = toFilesystemId
			pollResult.StartingCommit = fromSnap
			pollResult.TargetCommit = snapRange.toSnap.Id

			err = updatePollResult(transferRequestId, *pollResult)
			if err != nil {
				return &Event{
					Name: "push-initiator-cant-write-to-etcd", Args: &EventArgs{"err": err},
				}, backoffState
			}

			// tell the remote what snapshot to expect
			var result bool
			log.Printf("[retryPush] calling RegisterTransfer with args: %+v", pollResult)
			err = client.CallRemote(
				ctx, "DotmeshRPC.RegisterTransfer", pollResult, &result,
			)
			if err != nil {
				return &Event{
					Name: "push-initiator-cant-register-transfer", Args: &EventArgs{"err": err},
				}, backoffState
			}

			err = updatePollResult(transferRequestId, *pollResult)
			if err != nil {
				return &Event{
					Name: "push-initiator-cant-write-to-etcd", Args: &EventArgs{"err": err},
				}, backoffState
			}

			return f.push(
				fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
				snapRange, transferRequest, &transferRequestId, pollResult, client,
				ctx,
			)
		}()
		if responseEvent.Name == "finished-push" || responseEvent.Name == "peer-up-to-date" {
			log.Printf("[actualPush] Successful push!")
			return responseEvent, nextState
		}
		retry++
		f.updateTransfer(
			fmt.Sprintf("retry %d", retry),
			fmt.Sprintf("Attempting to push %s got %s", f.filesystemId, responseEvent),
		)
		log.Printf(
			"[retry attempt %d] squashing and retrying in %ds because we "+
				"got a %s (which tried to put us into state %p)...",
			retry, retry, responseEvent, nextState,
		)
		time.Sleep(time.Duration(retry) * time.Second)
	}
	log.Printf(
		"[actualPush] Maximum retry attempts exceeded, "+
			"returning latest error: %s (to move into state %p)",
		responseEvent, nextState,
	)
	return responseEvent, nextState
}
