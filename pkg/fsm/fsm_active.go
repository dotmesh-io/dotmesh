package fsm

import (
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"

	log "github.com/sirupsen/logrus"
)

func logNextState(from string) {
	log.WithField("from", from).Debug("[activeState] moving to next state")
}

func activeState(f *FsMachine) StateFn {
	f.transitionedTo("active", "waiting")
	log.Printf("entering active state for %s", f.filesystemId)
	select {
	case file := <-f.fileInputIO:
		r := f.saveFile(file)
		logNextState("fileInputIO")
		return r
	case file := <-f.fileOutputIO:
		r := f.readFile(file)
		logNextState("fileOutputIO")
		return r
	case e := <-f.innerRequests:
		if e.Name == "delete" {
			err := f.state.DeleteFilesystem(f.filesystemId)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-delete",
					Args: &types.EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &types.Event{
					Name: "deleted",
				}
			}
			logNextState("delete")
			return nil
		} else if e.Name == "predictSize" {

			fromFilesystemId := (*e.Args)["FromFilesystemId"].(string)
			fromSnapshotId := (*e.Args)["FromSnapshotId"].(string)
			toFilesystemId := (*e.Args)["ToFilesystemId"].(string)
			toSnapshotId := (*e.Args)["ToSnapshotId"].(string)

			size, err := f.zfs.PredictSize(
				fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
			)

			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "error-predict-size",
					Args: &types.EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &types.Event{
					Name: "predictedSize",
					Args: &types.EventArgs{"size": int64(size)},
				}
			}
			f.transitionedTo("active", "predicted size")
			logNextState("predictSize")
			return activeState
		} else if e.Name == "transfer" {

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("transfer 1")
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("transfer 2")
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT TRANSFER REQUEST %+v", f.lastTransferRequest)
			if f.lastTransferRequest.Direction == "push" {
				logNextState("transfer push")
				return pushInitiatorState
			} else if f.lastTransferRequest.Direction == "pull" {
				logNextState("transfer pull")
				return pullInitiatorState
			}
		} else if e.Name == "s3-transfer" {

			// TODO dedupe
			transferRequest, err := s3TransferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "s3-cant-cast-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("s3-transfer 1")
				return backoffState
			}
			f.lastS3TransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "s3-cant-cast-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("s3-transfer 2")
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT S3 TRANSFER REQUEST %+v", f.lastS3TransferRequest)
			if f.lastS3TransferRequest.Direction == "push" {
				logNextState("s3-transfer push")
				return s3PushInitiatorState
			} else if f.lastS3TransferRequest.Direction == "pull" {
				logNextState("s3-transfer pull")
				return s3PullInitiatorState
			}
		} else if e.Name == "peer-transfer" {

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("peer-transfer 1")
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("peer-transfer 2")
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT PEER TRANSFER REQUEST %+v", f.lastTransferRequest)
			if f.lastTransferRequest.Direction == "push" {
				logNextState("peer-transfer push")
				return pushPeerState
			} else if f.lastTransferRequest.Direction == "pull" {
				logNextState("peer-transfer pull")
				return pullPeerState
			}
		} else if e.Name == "move" {
			// move straight into a state which doesn't allow us to take
			// snapshots or do rollbacks
			// refuse to move if we have any containers running
			containers, err := f.containersRunning()
			if err != nil {
				log.Printf("[activeState:%s] Can't move filesystem while we can't list whether containers are using it. %s", f.filesystemId, err)
				f.innerResponses <- &types.Event{
					Name: "error-listing-containers-during-move",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("move 1")
				return backoffState
			}
			if len(containers) > 0 {
				log.Printf("[activeState:%s] Can't move filesystem when containers are using it.", f.filesystemId)
				f.innerResponses <- &types.Event{
					Name: "cannot-move-while-containers-running",
					Args: &types.EventArgs{"containers": containers},
				}
				logNextState("move 2")
				return backoffState
			}
			f.handoffRequest = e
			logNextState("move")
			return handoffState
		} else if e.Name == "fork" {
			response, state := f.fork(e)
			f.innerResponses <- response
			logNextState("fork")
			return state
		} else if e.Name == "diff" {
			response, state := f.diff(e)
			f.innerResponses <- response
			logNextState("diff")
			return state
		} else if e.Name == "snapshot" {
			response, state := f.snapshot(e)
			f.innerResponses <- response
			logNextState("snapshot")
			return state
		} else if e.Name == "mount-snapshot" {
			snapId := (*e.Args)["snapId"].(string)
			response, state := f.mountSnap(snapId, true)
			f.innerResponses <- response
			logNextState("mount-snapshot")
			return state
		} else if e.Name == "stash" {
			snapshotId := (*e.Args)["snapshotId"].(string)
			err := f.recoverFromDivergence(snapshotId)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "failed-stash",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("stash 1")
				return backoffState
			}
			f.innerResponses <- &types.Event{
				Name: "stashed",
				Args: &types.EventArgs{"NewBranchName": "TODO"},
			}
			logNextState("stash")
			return discoveringState
		} else if e.Name == "rollback" {
			// roll back to given snapshot
			rollbackTo := (*e.Args)["rollbackTo"].(string)
			// TODO also roll back slaves (i.e., support doing this in unmounted state)
			sliceIndex := -1
			for i, snapshot := range f.filesystem.Snapshots {
				if snapshot.Id == rollbackTo {
					// the first *deleted* snapshot will be the one *after*
					// rollbackTo
					sliceIndex = i + 1
				}
			}
			// XXX This is broken for pinned branches right now
			err := f.stopContainers()
			defer func() {
				err := f.startContainers()
				if err != nil {
					log.Printf(
						"[activeState] unable to start containers in deferred func: %s",
						err,
					)
				}
			}()
			if err != nil {
				log.Printf(
					"%v while trying to stop containers during rollback %s",
					err, f.zfs.FQ(f.filesystemId),
				)
				f.innerResponses <- &types.Event{
					Name: "failed-stop-containers-during-rollback",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("rollback 1")
				return backoffState
			}
			output, err := f.zfs.Rollback(f.filesystemId, rollbackTo)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "failed-rollback",
					Args: &types.EventArgs{"err": err, "combined-output": string(output)},
				}
				logNextState("rollback 2")
				return backoffState
			}
			if sliceIndex > 0 {
				log.Printf("found index %d", sliceIndex)
				log.Printf("snapshots before %+v", f.filesystem.Snapshots)

				f.snapshotsLock.Lock()
				f.filesystem.Snapshots = f.filesystem.Snapshots[:sliceIndex]
				f.snapshotsLock.Unlock()

				err = f.snapshotsChanged()
				if err != nil {
					log.Printf("%v while trying to report that snapshots have changed %s", err, f.zfs.FQ(f.filesystemId))
					f.innerResponses <- &types.Event{
						Name: "failed-rollback-snapshots-changed",
						Args: &types.EventArgs{"err": err},
					}
					logNextState("rollback 3")
					return backoffState
				}
				log.Printf("snapshots after %+v", f.filesystem.Snapshots)
			} else {
				f.innerResponses <- &types.Event{
					Name: "no-such-snapshot",
				}
			}
			err = f.startContainers()
			if err != nil {
				log.Printf(
					"%v while trying to start containers during rollback %s",
					err, f.zfs.FQ(f.filesystemId),
				)
				f.innerResponses <- &types.Event{
					Name: "failed-start-containers-during-rollback",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("rollback 4")
				return backoffState
			}
			f.innerResponses <- &types.Event{
				Name: "rolled-back",
			}
			f.transitionedTo("active", "rolled back")
			logNextState("rollback")
			return activeState
		} else if e.Name == "clone" {
			// clone a new filesystem from the given snapshot, then spin off a
			// new fsMachine for it.

			/*
				"topLevelFilesystemId": topLevelFilesystemId,
				"originFilesystemId":   originFilesystemId,
				"originSnapshotId":     args.SourceSnapshotId,
				"newBranchName":        args.NewBranchName,
			*/

			topLevelFilesystemId := (*e.Args)["topLevelFilesystemId"].(string)
			originFilesystemId := (*e.Args)["originFilesystemId"].(string)
			originSnapshotId := (*e.Args)["originSnapshotId"].(string)
			newBranchName := (*e.Args)["newBranchName"].(string)

			newCloneFilesystemId := uuid.New().String()
			output, err := f.zfs.Clone(f.filesystemId, originSnapshotId, newCloneFilesystemId)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "failed-clone",
					Args: &types.EventArgs{"err": err, "combined-output": string(output)},
				}
				logNextState("clone 1")
				return backoffState
			}

			errorName, err := f.state.ActivateClone(topLevelFilesystemId, originFilesystemId, originSnapshotId, newCloneFilesystemId, newBranchName)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: errorName, Args: &types.EventArgs{"err": err},
				}
				logNextState("clone 2")
				return backoffState
			}

			f.innerResponses <- &types.Event{
				Name: "cloned",
				Args: &types.EventArgs{"newFilesystemId": newCloneFilesystemId},
			}
			f.transitionedTo("active", "cloned")
			logNextState("clone")
			return activeState
		} else if e.Name == "mount" {
			f.innerResponses <- &types.Event{
				Name: "mounted",
				Args: &types.EventArgs{},
			}
			f.transitionedTo("active", "mounted")
			logNextState("mount")
			return activeState
		} else if e.Name == "unmount" {
			// fail if any containers running
			containers, err := f.containersRunning()
			if err != nil {
				log.Printf("[activeState:%s] %s Can't unmount filesystem when we are unable to list containers using it", f.filesystemId, err)
				f.innerResponses <- &types.Event{
					Name: "error-listing-containers-during-unmount",
					Args: &types.EventArgs{"err": err},
				}
				logNextState("unmount 1")
				return backoffState
			}
			if len(containers) > 0 {
				log.Printf("[activeState:%s] Can't unmount filesystem while containers are using it", f.filesystemId)
				f.innerResponses <- &types.Event{
					Name: "cannot-unmount-while-running-containers",
					Args: &types.EventArgs{"containers": containers},
				}
				logNextState("unmount 2")
				return backoffState
			}
			response, state := f.unmount()
			f.innerResponses <- response
			logNextState("unmount")
			return state
		} else {
			f.innerResponses <- &types.Event{
				Name: "unhandled",
				Args: &types.EventArgs{"current-state": f.currentState, "types.Event": e},
			}
			log.Printf("unhandled types.Event %s while in activeState", e)
		}
	}
	// something unknown happened, go and check the state of the system after a
	// short timeout to avoid busylooping
	logNextState("fallthrough")
	return backoffState
}
