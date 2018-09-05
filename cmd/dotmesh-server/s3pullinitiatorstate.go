package main

import (
	"fmt"
	"github.com/nu7hatch/gouuid"
	"os"
)

func s3PullInitiatorState(f *fsMachine) stateFn {
	f.transitionedTo("s3PullInitiatorState", "requesting")
	transferRequest := f.lastS3TransferRequest
	transferRequestId := f.lastTransferRequestId
	pollResult := TransferPollResult{
		TransferRequestId: transferRequestId,
		Direction:         transferRequest.Direction,
		InitiatorNodeId:   f.state.myNodeId,
		Index:             1,
		Status:            "starting",
	}
	containers, err := f.containersRunning()
	if err != nil {
		f.errorDuringTransfer("error-listing-containers-during-pull", err)
		return backoffState
	}
	if len(containers) > 0 {
		f.sendArgsEventUpdateUser(&EventArgs{"containers": containers}, "cannot-pull-while-containers-running", "Can't pull into filesystem while containers are using it", pollResult)
		return backoffState
	}

	// create the default paths
	destPath := fmt.Sprintf("%s/%s", mnt(f.filesystemId), "__default__")
	err = os.MkdirAll(destPath, 0775)
	if err != nil {
		f.errorDuringTransfer("cannot-create-default-dir", err)
		return backoffState
	}
	versionsPath := fmt.Sprintf("%s/%s", mnt(f.filesystemId), "dm.s3-versions")
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

	f.lastPollResult = &pollResult
	err = updatePollResult(transferRequestId, pollResult)
	if err != nil {
		f.sendEvent(&EventArgs{"err": err}, "s3-pull-initiator-cant-write-to-etcd", "cannot write to etcd")
		return backoffState
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

		if err != nil && !os.IsNotExist(err) {
			f.errorDuringTransfer("s3-pull-initiator-cant-read-metadata", err)
			return backoffState
		}
	}
	bucketChanged, keyVersions, err := downloadS3Bucket(svc, transferRequest.RemoteName, destPath, transferRequestId, transferRequest.Prefixes, &pollResult, latestMeta)
	if err != nil {
		f.errorDuringTransfer("cant-pull-from-s3", err)
		return backoffState
	}
	if bucketChanged {
		id, err := uuid.NewV4()
		if err != nil {
			f.errorDuringTransfer("failed-uuid", err)
			return backoffState
		}
		snapshotId := id.String()
		path := fmt.Sprintf("%s/%s", mnt(f.filesystemId), "dm.s3-versions")
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
		response, _ := f.snapshot(&Event{Name: "snapshot",
			Args: &EventArgs{"metadata": metadata{"message": "s3 content"},
				"snapshotId": snapshotId}})
		if response.Name != "snapshotted" {
			f.innerResponses <- response
			err = updateUser("Could not take snapshot", transferRequestId, pollResult)
			if err != nil {
				f.sendEvent(&EventArgs{"err": err}, "cant-write-to-etcd", "cant write to etcd")
			}
			return backoffState
		}
	}
	pollResult.Status = "finished"
	pollResult.Index = pollResult.Total
	err = updatePollResult(transferRequestId, pollResult)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "s3-pull-initiator-cant-write-to-etcd",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	f.innerResponses <- &Event{
		Name: "s3-transferred",
		Args: &EventArgs{},
	}
	return discoveringState
}
