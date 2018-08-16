package main

import (
	"fmt"
	"log"
	"os"
)

func s3PushInitiatorState(f *fsMachine) stateFn {
	f.transitionedTo("s3PushInitiatorState", "requesting")
	transferRequest := f.lastS3TransferRequest
	transferRequestId := f.lastTransferRequestId
	pollResult := TransferPollResult{
		TransferRequestId: transferRequestId,
		Direction:         transferRequest.Direction,
		InitiatorNodeId:   f.state.myNodeId,
		Index:             0,
		Status:            "starting",
	}
	f.lastPollResult = &pollResult
	err := updatePollResult(transferRequestId, pollResult)
	if err != nil {
		f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "S3 push initiator couldn't write to etcd")
		return backoffState
	}
	latestSnap, err := f.getLastNonMetadataSnapshot()
	if err != nil {
		f.errorDuringTransfer("s3-push-initiator-cant-get-snapshot-data", err)
		return backoffState
	}
	event, _ := f.mountSnap(latestSnap.Id, true)
	if event.Name != "mounted" {
		f.innerResponses <- event
		updateUser("Could not mount filesystem@commit readonly", transferRequestId, pollResult)
		return backoffState
	}
	mountPoint := mnt(fmt.Sprintf("%s@%s", f.filesystemId, latestSnap.Id))

	snaps, err := f.state.snapshotsForCurrentMaster(f.filesystemId)
	if len(snaps) == 0 {
		f.innerResponses <- event
		updateUser("No commits to push!", transferRequestId, pollResult)
		return backoffState
	}
	metadataSnap := snaps[len(snaps)-1]
	pathToS3Metadata := fmt.Sprintf("%s@%s/dm.s3-versions/%s", mnt(f.filesystemId), metadataSnap.Id, latestSnap.Id)
	log.Printf("[s3PushInitiatorState] path to s3 metadata: %s", pathToS3Metadata)
	event, _ = f.mountSnap(metadataSnap.Id, true)
	if event.Name != "mounted" {
		f.innerResponses <- event
		updateUser("Could not mount filesystem@commit readonly", transferRequestId, pollResult)
		return backoffState
	}
	if latestSnap != nil {
		if _, err := os.Stat(pathToS3Metadata); err == nil {
			f.sendArgsEventUpdateUser(&EventArgs{"path": pathToS3Metadata}, "commit-already-in-s3", "Found s3 metadata for latest snap - nothing to push!", pollResult)
			return discoveringState
		} else if !os.IsNotExist(err) {
			f.errorDuringTransfer("couldnt-stat-s3-meta-file", err)
			return backoffState
		}

		svc, err := getS3Client(transferRequest)
		if err != nil {
			f.errorDuringTransfer("couldnt-connect-to-s3", err)
			return backoffState
		}
		// list everything in the main directory
		pathToMount := fmt.Sprintf("%s/__default__", mountPoint)
		paths, dirSize, err := getKeysForDir(pathToMount, "")
		// push everything to s3
		pollResult.Total = len(paths)
		pollResult.Size = dirSize
		pollResult.Status = "beginning upload"
		err = updatePollResult(transferRequestId, pollResult)
		if err != nil {
			f.sendEvent(&EventArgs{"err": err}, "cant-write-etcd", "")
			return backoffState
		}
		keyToVersionIds := make(map[string]string)
		keyToVersionIds, err = updateS3Files(keyToVersionIds, paths, pathToMount, transferRequestId, transferRequest.RemoteName, transferRequest.Prefixes, svc, pollResult)
		if err != nil {
			f.errorDuringTransfer("error-updating-s3-objects", err)
			return backoffState
		}
		keyToVersionIds, err = removeOldS3Files(keyToVersionIds, paths, transferRequest.RemoteName, transferRequest.Prefixes, svc)
		if err != nil {
			f.errorDuringTransfer("error-during-object-pagination", err)
			return backoffState
		}
		// event, _ = f.unmountSnap(latestSnap.Id)
		// if event.Name != "unmounted" {
		// 	f.innerResponses <- event
		// 	updateUser("Could not unmount filesystem@commit", transferRequestId, pollResult)
		// 	return backoffState
		// }
		// check if there is anything in s3 that isn't in this list - if there is, delete it
		// create a file under the last commit id in the appropriate place + dump out the new versions to it
		dirtyPathToS3Meta := fmt.Sprintf("%s/dm.s3-versions/%s", mnt(f.filesystemId), latestSnap.Id)
		err = writeS3Metadata(dirtyPathToS3Meta, keyToVersionIds)
		if err != nil {
			f.errorDuringTransfer("couldnt-write-s3-metadata-push", err)
			return backoffState
		}

		// create a new commit with the type "dotmesh.metadata_only" so that we can ignore it when detecting new commits
		response, _ := f.snapshot(&Event{
			Name: "snapshot",
			Args: &EventArgs{"metadata": metadata{
				"message": "adding s3 metadata",
				"type":    "dotmesh.metadata_only",
			},
			},
		})
		if response.Name != "snapshotted" {
			f.innerResponses <- response
			err = updateUser("Could not take snapshot", transferRequestId, pollResult)
			if err != nil {
				f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "cant write to etcd")
			}
			return backoffState
		}
		// unmount the current commit

		// put something in S3 to let us know the filesystem ID/other dotmesh details in case we need to recover at a later stage
	}
	// todo set this to something more reasonable and do stuff with all of the above
	pollResult.Status = "finished"
	pollResult.Total = pollResult.Index
	f.lastPollResult = &pollResult
	err = updatePollResult(transferRequestId, pollResult)
	if err != nil {
		f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "S3 push initiator couldn't write to etcd")
		return backoffState
	}
	f.innerResponses <- &Event{
		Name: "s3-pushed",
	}
	return discoveringState
}
