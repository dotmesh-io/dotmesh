package main

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"log"
	"os/exec"
)

func activeState(f *fsMachine) stateFn {
	f.transitionedTo("active", "waiting")
	log.Printf("entering active state for %s", f.filesystemId)
	select {
	case e := <-f.innerRequests:
		if e.Name == "delete" {
			err := f.state.deleteFilesystem(f.filesystemId)
			if err != nil {
				f.innerResponses <- &Event{
					Name: "cant-delete",
					Args: &EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &Event{
					Name: "deleted",
				}
			}
			return nil
		} else if e.Name == "predictSize" {

			fromFilesystemId := (*e.Args)["FromFilesystemId"].(string)
			fromSnapshotId := (*e.Args)["FromSnapshotId"].(string)
			toFilesystemId := (*e.Args)["ToFilesystemId"].(string)
			toSnapshotId := (*e.Args)["ToSnapshotId"].(string)

			size, err := predictSize(
				fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
			)

			if err != nil {
				f.innerResponses <- &Event{
					Name: "error-predict-size",
					Args: &EventArgs{"err": err},
				}
			} else {
				f.innerResponses <- &Event{
					Name: "predictedSize",
					Args: &EventArgs{"size": int64(size)},
				}
			}
			return activeState
		} else if e.Name == "put-file" {
			//filename := (*e.Args)["key"].(string)
			log.Println(e.Args)
			s3ApiRequest, err := s3ApiRequestify((*e.Args)["S3Request"])
			if err != nil {
				log.Printf("%v while trying to cast to s3request", err)
				f.innerResponses <- &Event{
					Name: "failed-s3apirequest-cast",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			return f.saveFile(s3ApiRequest)
		} else if e.Name == "transfer" {

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &Event{
					Name: "cant-cast-transfer-request",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &Event{
					Name: "cant-cast-transfer-requestid",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT TRANSFER REQUEST %+v", f.lastTransferRequest)
			if f.lastTransferRequest.Direction == "push" {
				return pushInitiatorState
			} else if f.lastTransferRequest.Direction == "pull" {
				return pullInitiatorState
			}
		} else if e.Name == "s3-transfer" {

			// TODO dedupe
			transferRequest, err := s3TransferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &Event{
					Name: "s3-cant-cast-transfer-request",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastS3TransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &Event{
					Name: "s3-cant-cast-transfer-requestid",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT S3 TRANSFER REQUEST %+v", f.lastS3TransferRequest)
			if f.lastS3TransferRequest.Direction == "push" {
				return s3PushInitiatorState
			} else if f.lastS3TransferRequest.Direction == "pull" {
				return s3PullInitiatorState
			}
		} else if e.Name == "peer-transfer" {

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &Event{
					Name: "cant-cast-transfer-request",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &Event{
					Name: "cant-cast-transfer-requestid",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			log.Printf("GOT PEER TRANSFER REQUEST %+v", f.lastTransferRequest)
			if f.lastTransferRequest.Direction == "push" {
				return pushPeerState
			} else if f.lastTransferRequest.Direction == "pull" {
				return pullPeerState
			}
		} else if e.Name == "move" {
			// move straight into a state which doesn't allow us to take
			// snapshots or do rollbacks
			// refuse to move if we have any containers running
			containers, err := f.containersRunning()
			if err != nil {
				log.Printf("[activeState:%s] Can't move filesystem while we can't list whether containers are using it. %s", f.filesystemId, err)
				f.innerResponses <- &Event{
					Name: "error-listing-containers-during-move",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			if len(containers) > 0 {
				log.Printf("[activeState:%s] Can't move filesystem when containers are using it.", f.filesystemId)
				f.innerResponses <- &Event{
					Name: "cannot-move-while-containers-running",
					Args: &EventArgs{"containers": containers},
				}
				return backoffState
			}
			f.handoffRequest = e
			return handoffState
		} else if e.Name == "snapshot" {
			response, state := f.snapshot(e)
			f.innerResponses <- response
			return state
		} else if e.Name == "mount-snapshot" {
			snapId := (*e.Args)["snapId"].(string)
			response, state := f.mountSnap(snapId, true)
			f.innerResponses <- response
			return state
		} else if e.Name == "stash" {
			snapshotId := (*e.Args)["snapshotId"].(string)
			e := f.recoverFromDivergence(snapshotId)
			if e != nil {
				f.innerResponses <- &Event{
					Name: "failed-stash",
					Args: &EventArgs{"err": e},
				}
				return backoffState
			}
			f.innerResponses <- &Event{
				Name: "stashed",
				Args: &EventArgs{"NewBranchName": "TODO"},
			}
			return discoveringState
		} else if e.Name == "rollback" {
			// roll back to given snapshot
			rollbackTo := (*e.Args)["rollbackTo"].(string)
			// TODO also roll back slaves (i.e., support doing this in unmounted state)
			sliceIndex := -1
			for i, snapshot := range f.filesystem.snapshots {
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
					err, fq(f.filesystemId),
				)
				f.innerResponses <- &Event{
					Name: "failed-stop-containers-during-rollback",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			logZFSCommand(f.filesystemId, fmt.Sprintf("%s rollback -r %s@%s", ZFS, fq(f.filesystemId), rollbackTo))
			out, err := exec.Command(ZFS, "rollback",
				"-r", fq(f.filesystemId)+"@"+rollbackTo).CombinedOutput()
			if err != nil {
				log.Printf("%v while trying to rollback %s", err, fq(f.filesystemId))
				f.innerResponses <- &Event{
					Name: "failed-rollback",
					Args: &EventArgs{"err": err, "combined-output": string(out)},
				}
				return backoffState
			}
			if sliceIndex > 0 {
				log.Printf("found index %d", sliceIndex)
				log.Printf("snapshots before %+v", f.filesystem.snapshots)
				func() {
					f.snapshotsLock.Lock()
					defer f.snapshotsLock.Unlock()
					f.filesystem.snapshots = f.filesystem.snapshots[:sliceIndex]
				}()
				err = f.snapshotsChanged()
				if err != nil {
					log.Printf("%v while trying to report that snapshots have changed %s", err, fq(f.filesystemId))
					f.innerResponses <- &Event{
						Name: "failed-rollback-snapshots-changed",
						Args: &EventArgs{"err": err},
					}
					return backoffState
				}
				log.Printf("snapshots after %+v", f.filesystem.snapshots)
			} else {
				f.innerResponses <- &Event{
					Name: "no-such-snapshot",
				}
			}
			err = f.startContainers()
			if err != nil {
				log.Printf(
					"%v while trying to start containers during rollback %s",
					err, fq(f.filesystemId),
				)
				f.innerResponses <- &Event{
					Name: "failed-start-containers-during-rollback",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.innerResponses <- &Event{
				Name: "rolled-back",
			}
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

			uuid, err := uuid.NewV4()
			if err != nil {
				f.innerResponses <- &Event{
					Name: "failed-uuid", Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			newCloneFilesystemId := uuid.String()

			logZFSCommand(f.filesystemId, fmt.Sprintf("%s clone %s@%s %s", ZFS, fq(f.filesystemId), originSnapshotId, fq(newCloneFilesystemId)))
			out, err := exec.Command(
				ZFS, "clone",
				fq(f.filesystemId)+"@"+originSnapshotId,
				fq(newCloneFilesystemId),
			).CombinedOutput()
			if err != nil {
				log.Printf("%v while trying to clone %s", err, fq(f.filesystemId))
				f.innerResponses <- &Event{
					Name: "failed-clone",
					Args: &EventArgs{"err": err, "combined-output": string(out)},
				}
				return backoffState
			}

			errorName, err := activateClone(f.state,
				topLevelFilesystemId, originFilesystemId, originSnapshotId,
				newCloneFilesystemId, newBranchName)
			if err != nil {
				f.innerResponses <- &Event{
					Name: errorName, Args: &EventArgs{"err": err},
				}
				return backoffState
			}

			f.innerResponses <- &Event{
				Name: "cloned",
				Args: &EventArgs{"newFilesystemId": newCloneFilesystemId},
			}
			return activeState
		} else if e.Name == "mount" {
			f.innerResponses <- &Event{
				Name: "mounted",
				Args: &EventArgs{},
			}
			return activeState
		} else if e.Name == "unmount" {
			// fail if any containers running
			containers, err := f.containersRunning()
			if err != nil {
				log.Printf("[activeState:%s] %s Can't unmount filesystem when we are unable to list containers using it", f.filesystemId, err)
				f.innerResponses <- &Event{
					Name: "error-listing-containers-during-unmount",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			if len(containers) > 0 {
				log.Printf("[activeState:%s] Can't unmount filesystem while containers are using it", f.filesystemId)
				f.innerResponses <- &Event{
					Name: "cannot-unmount-while-running-containers",
					Args: &EventArgs{"containers": containers},
				}
				return backoffState
			}
			response, state := f.unmount()
			f.innerResponses <- response
			return state
		} else {
			f.innerResponses <- &Event{
				Name: "unhandled",
				Args: &EventArgs{"current-state": f.currentState, "event": e},
			}
			log.Printf("unhandled event %s while in activeState", e)
		}
	}
	// something unknown happened, go and check the state of the system after a
	// short timeout to avoid busylooping
	return backoffState
}
