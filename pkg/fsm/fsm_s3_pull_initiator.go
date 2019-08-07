package fsm

import (
	"fmt"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"
)

func s3PullInitiatorState(f *FsMachine) StateFn {
	f.transitionedTo("s3PullInitiatorState", "requesting")
	transferRequest := f.lastS3TransferRequest
	transferRequestId := f.lastTransferRequestId
	containers, err := f.containersRunning()
	if err != nil {
		f.errorDuringTransfer("error-listing-containers-during-pull", err)
		return backoffState
	}
	if len(containers) > 0 {
		f.sendArgsEventUpdateUser(&types.EventArgs{"containers": containers}, "cannot-pull-while-containers-running", "Can't pull into filesystem while containers are using it")
		return backoffState
	}

	// create the default paths
	destPath := fmt.Sprintf("%s/%s", utils.Mnt(f.filesystemId), "__default__")
	err = os.MkdirAll(destPath, 0775)
	if err != nil {
		f.errorDuringTransfer("cannot-create-default-dir", err)
		return backoffState
	}
	versionsPath := fmt.Sprintf("%s/%s", utils.Mnt(f.filesystemId), "dm.s3-versions")
	err = os.MkdirAll(versionsPath, 0775)
	if err != nil {
		f.errorDuringTransfer("cannot-create-versions-metadata-dir", err)
		return backoffState
	}
	svc, err := getS3Client(transferRequest)
	if err != nil {
		f.errorDuringTransfer("couldnt-create-s3-client", err)
		return backoffState
	}

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

	latestMeta := make(map[string]string)
	latestSnap, err := f.getLastNonMetadataSnapshot()
	if err != nil {
		f.errorDuringTransfer("s3-pull-initiator-cant-get-snapshot-data", err)
		return backoffState
	}
	if latestSnap != nil {
		// todo:
		// if "type" == "metadata-only" in commit ignore it
		// go back to the one before it until we find one that isn't that type
		err := loadS3Meta(f.filesystemId, latestSnap.Id, &latestMeta)

		if err != nil {
			if os.IsNotExist(err) {
				f.errorDuringTransfer("must push before pulling!", err)
				return backoffState
			} else {
				f.errorDuringTransfer("s3-pull-initiator-cant-read-metadata", err)
				return backoffState
			}
		}
	}
	bucketChanged, keyVersions, err := downloadS3Bucket(f, svc, transferRequest.RemoteName, destPath, transferRequestId, transferRequest.Prefixes, latestMeta)
	if err != nil {
		f.errorDuringTransfer("cant-pull-from-s3", err)
		return backoffState
	}
	if bucketChanged {
		snapshotId := uuid.New().String()
		path := fmt.Sprintf("%s/%s", utils.Mnt(f.filesystemId), "dm.s3-versions")
		err = os.MkdirAll(path, 0775)
		if err != nil {
			f.errorDuringTransfer("couldnt-create-metadata-subdot", err)
			return backoffState
		}
		pathToCommitMeta := fmt.Sprintf("%s/%s", path, snapshotId)
		err = writeS3Metadata(pathToCommitMeta, keyVersions)
		if err != nil {
			f.errorDuringTransfer("couldnt-write-s3-metadata-pull", err)
		}
		response, _ := f.snapshot(&types.Event{Name: "snapshot",
			Args: &types.EventArgs{"metadata": map[string]string{"message": "s3 content"},
				"snapshotId": snapshotId}})
		if response.Name != "snapshotted" {
			f.innerResponses <- response
			err = f.updateUser("Could not take snapshot")
			if err != nil {
				f.sendEvent(&types.EventArgs{"err": err}, "cant-write-to-etcd", "cant write to etcd")
			}
			return backoffState
		}
	}

	f.transferUpdates <- types.TransferUpdate{
		Kind: types.TransferFinished,
	}

	f.innerResponses <- &types.Event{
		Name: "s3-transferred",
		Args: &types.EventArgs{},
	}
	return discoveringState
}
