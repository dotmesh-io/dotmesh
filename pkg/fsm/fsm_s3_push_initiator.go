package fsm

import (
	"fmt"
	"log"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
)

func s3PushInitiatorState(f *FsMachine) StateFn {
	f.transitionedTo("s3PushInitiatorState", "requesting")
	transferRequest := f.lastS3TransferRequest
	transferRequestId := f.lastTransferRequestId

	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferStart,
		Changes: types.TransferPollResult{
			TransferRequestId: transferRequestId,
			Direction:         transferRequest.Direction,
			InitiatorNodeId:   f.state.NodeID(),
			Index:             0,
			Status:            "starting",
		},
	}

	latestSnap, err := f.getLastNonMetadataSnapshot()
	if err != nil {
		f.errorDuringTransfer("s3-push-initiator-cant-get-snapshot-data", err)
		return backoffState
	}
	event, _ := f.mountSnap(latestSnap.Id, true)
	if event.Name != "mounted" {
		f.innerResponses <- event
		f.updateUser("Could not mount filesystem@commit readonly")
		return backoffState
	}
	mountPoint := utils.Mnt(fmt.Sprintf("%s@%s", f.filesystemId, latestSnap.Id))

	snaps, err := f.state.SnapshotsForCurrentMaster(f.filesystemId)
	if len(snaps) == 0 {
		f.innerResponses <- event
		f.updateUser("No commits to push!")
		return backoffState
	}
	metadataSnap := snaps[len(snaps)-1]
	pathToS3Metadata := fmt.Sprintf("%s@%s/dm.s3-versions/%s", utils.Mnt(f.filesystemId), metadataSnap.Id, latestSnap.Id)
	log.Printf("[s3PushInitiatorState] path to s3 metadata: %s", pathToS3Metadata)
	event, _ = f.mountSnap(metadataSnap.Id, true)
	if event.Name != "mounted" {
		f.innerResponses <- event
		f.updateUser("Could not mount filesystem@commit readonly")
		return backoffState
	}
	if latestSnap != nil {
		if _, err := os.Stat(pathToS3Metadata); err == nil {
			f.sendArgsEventUpdateUser(&types.EventArgs{"path": pathToS3Metadata}, "commit-already-in-s3", "Found s3 metadata for latest snap - nothing to push!")
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
		if err != nil {
			f.sendEvent(&types.EventArgs{"err": err, "path": pathToMount}, "cant-get-keys-for-directory", "")
			return backoffState
		}
		// push everything to s3
		f.transferUpdates <- types.TransferUpdate{
			Kind: types.TransferTotalAndSize,
			Changes: types.TransferPollResult{
				Status: "beginning upload",
				Total:  len(paths),
				Size:   dirSize,
			},
		}

		keyToVersionIds := make(map[string]string)
		keyToVersionIds, err = updateS3Files(f, keyToVersionIds, paths, pathToMount, transferRequestId, transferRequest.RemoteName, transferRequest.Prefixes, svc)
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
		directoryPath := fmt.Sprintf("%s/dm.s3-versions", utils.Mnt(f.filesystemId))
		dirtyPathToS3Meta := fmt.Sprintf("%s/%s", directoryPath, latestSnap.Id)
		err = os.MkdirAll(directoryPath, 0775)
		if err != nil {
			f.errorDuringTransfer("couldnt-create-metadata-subdot", err)
			return backoffState
		}
		err = writeS3Metadata(dirtyPathToS3Meta, keyToVersionIds)
		if err != nil {
			f.errorDuringTransfer("couldnt-write-s3-metadata-push", err)
			return backoffState
		}

		// create a new commit with the type "dotmesh.metadata_only" so that we can ignore it when detecting new commits
		response, _ := f.snapshot(&types.Event{
			Name: "snapshot",
			Args: &types.EventArgs{"metadata": types.Metadata{
				"message": "adding s3 metadata",
				"type":    "dotmesh.metadata_only",
			},
			},
		})
		if response.Name != "snapshotted" {
			f.innerResponses <- response
			err = f.updateUser("Could not take snapshot")
			if err != nil {
				f.sendEvent(&types.EventArgs{"err": err}, "cant-write-to-etcd", "cant write to etcd")
			}
			return backoffState
		}
		// unmount the current commit

		// put something in S3 to let us know the filesystem ID/other dotmesh details in case we need to recover at a later stage
	}
	// todo set this to something more reasonable and do stuff with all of the above
	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferFinished,
	}

	f.innerResponses <- &types.Event{
		Name: "s3-pushed",
	}
	return discoveringState
}
