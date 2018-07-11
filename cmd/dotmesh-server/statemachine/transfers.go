package statemachine

// stuff used to do transfers, both for DM and S3

func s3TransferRequestify(in interface{}) (S3TransferRequest, error) {
	typed, ok := in.(map[string]interface{})
	if !ok {
		log.Printf("[s3TransferRequestify] Unable to cast %s to map[string]interface{}", in)
		return S3TransferRequest{}, fmt.Errorf(
			"Unable to cast %s to map[string]interface{}", in,
		)
	}
	return S3TransferRequest{
		KeyID:           typed["KeyID"].(string),
		SecretKey:       typed["SecretKey"].(string),
		Endpoint:        typed["Endpoint"].(string),
		Direction:       typed["Direction"].(string),
		LocalNamespace:  typed["LocalNamespace"].(string),
		LocalName:       typed["LocalName"].(string),
		LocalBranchName: typed["LocalBranchName"].(string),
		RemoteName:      typed["RemoteName"].(string),
	}, nil
}

func transferRequestify(in interface{}) (TransferRequest, error) {
	typed, ok := in.(map[string]interface{})
	if !ok {
		log.Printf("[transferRequestify] Unable to cast %s to map[string]interface{}", in)
		return TransferRequest{}, fmt.Errorf(
			"Unable to cast %s to map[string]interface{}", in,
		)
	}
	var port int
	if typed["Port"] == nil {
		port = 0
	} else {
		port = int(typed["Port"].(float64))
	}
	return TransferRequest{
		Peer:             typed["Peer"].(string),
		User:             typed["User"].(string),
		ApiKey:           typed["ApiKey"].(string),
		Port:             port,
		Direction:        typed["Direction"].(string),
		LocalNamespace:   typed["LocalNamespace"].(string),
		LocalName:        typed["LocalName"].(string),
		LocalBranchName:  typed["LocalBranchName"].(string),
		RemoteNamespace:  typed["RemoteNamespace"].(string),
		RemoteName:       typed["RemoteName"].(string),
		RemoteBranchName: typed["RemoteBranchName"].(string),
		TargetCommit:     typed["TargetCommit"].(string),
	}, nil
}

// for each clone, ensure its origin snapshot exists on the remote. if it
// doesn't, transfer it.
func (f *fsMachine) applyPath(
	path PathToTopLevelFilesystem, transferFn transferFn,
	transferRequestId string, pollResult *TransferPollResult,
	client *JsonRpcClient, transferRequest *TransferRequest,
) (*Event, stateFn) {
	/*
		Case 1: single master filesystem
		--------------------------------

		TopLevelFilesystemId: <master branch filesystem id>
		TopLevelFilesystemName: foo
		Clones: []

		transferFn("", "", "<master branch filesystem id>", "")

		Case 2: branch-of-branch-of-master (for example)
		------------------------------------------------

		TopLevelFilesystemId: <master branch filesystem id>
		TopLevelFilesystemName: foo
		Clones: []Clone{
			Clone{
				FilesystemId: <branch1 filesystem id>,
				Origin: {
					FilesystemId: <master branch filesystem id>,
					SnapshotId: <snapshot that is origin on master branch>,
				}
			},
			Clone{
				FilesystemId: <branch2 filesystem id>,
				Origin: {
					FilesystemId: <branch1 filesystem id>,
					SnapshotId: <snapshot that is origin on branch1 branch>,
				}
			},
		}

		Required actions:

		push master branch from:
			beginning to:
				snapshot that is origin on master branch
		push branch1 from:
			snapshot that is origin on master branch, to:
				snapshot that is origin on branch1 branch
		push branch2 from:
			snapshot that is origin on branch1 branch, to:
				latest snapshot on branch2

		Examples:

		transferFn("", "", "<master branch filesystem id>", "<origin snapshot on master>")

		push master branch from:
			beginning to:
				snapshot that is origin on master branch

		transferFn(
			"<master branch filesystem id>", "<origin snapshot on master>",
			"<branch1 filesystem id>", "<origin snapshot on branch1>",
		)

		push branch1 from:
			snapshot that is origin on master branch, to:
				snapshot that is origin on branch1 branch

		transferFn(
			"<branch1 branch filesystem id>", "<origin snapshot on branch1>",
			"<branch2 filesystem id>", "",
		)

		push branch2 from:
			snapshot that is origin on branch1 branch, to:
				latest snapshot on branch2
	*/

	var responseEvent *Event
	var nextState stateFn
	var firstSnapshot string

	log.Printf("[applyPath] applying path %#v", path)

	if len(path.Clones) == 0 {
		// just pushing a master branch to its latest snapshot
		// do a push with empty origin and empty target snapshot
		// TODO parametrize "push to snapshot" and expose in the UI
		firstSnapshot = ""
	} else {
		// push the master branch up to the first snapshot
		firstSnapshot = path.Clones[0].Clone.Origin.SnapshotId
	}
	log.Printf(
		"[applyPath,b] calling transferFn with fF=%v, fS=%v, tF=%v, tS=%v",
		"", "", path.TopLevelFilesystemId, firstSnapshot,
	)
	responseEvent, nextState = transferFn(f,
		"", "", path.TopLevelFilesystemId, firstSnapshot,
		transferRequestId, pollResult, client, transferRequest,
	)
	if !(responseEvent.Name == "finished-push" ||
		responseEvent.Name == "finished-pull" || responseEvent.Name == "peer-up-to-date") {
		msg := fmt.Sprintf(
			"Response event != finished-{push,pull} or peer-up-to-date: %s", responseEvent,
		)
		f.updateTransfer("error", msg)
		return &Event{
			Name: "error-in-attempting-apply-path",
			Args: &EventArgs{
				"error": msg,
			},
		}, backoffState
	}
	err := f.state.alignMountStateWithMasters(path.TopLevelFilesystemId)
	if err != nil {
		return &Event{
			Name: "error-maybe-mounting-filesystem",
			Args: &EventArgs{"error": err, "filesystemId": path.TopLevelFilesystemId},
		}, backoffState
	}
	err = f.incrementPollResultIndex(transferRequestId, pollResult)
	if err != nil {
		return &Event{Name: "error-incrementing-poll-result",
			Args: &EventArgs{"error": err}}, backoffState
	}

	for i, clone := range path.Clones {
		// default empty-strings is fine
		nextOrigin := Origin{}
		// is there a next (i+1'th) item? (i is zero-indexed)
		if len(path.Clones) > i+1 {
			// example: path.Clones is 2 items long, and we're on the second
			// one; i=1, len(path.Clones) = 2; 2 > 2 is false; so we're on the
			// last item so the guard evaluates to false; if we're on the first
			// item, 2 > 1 is true, so guard is true.
			nextOrigin = path.Clones[i+1].Clone.Origin
		}
		log.Printf(
			"[applyPath,i] calling transferFn with fF=%v, fS=%v, tF=%v, tS=%v",
			clone.Clone.Origin.FilesystemId, clone.Clone.Origin.SnapshotId,
			clone.Clone.FilesystemId, nextOrigin.SnapshotId,
		)
		responseEvent, nextState = transferFn(f,
			clone.Clone.Origin.FilesystemId, clone.Clone.Origin.SnapshotId,
			clone.Clone.FilesystemId, nextOrigin.SnapshotId,
			transferRequestId, pollResult, client, transferRequest,
		)
		if !(responseEvent.Name == "finished-push" ||
			responseEvent.Name == "finished-pull" || responseEvent.Name == "peer-up-to-date") {
			msg := fmt.Sprintf(
				"Response event != finished-{push,pull} or peer-up-to-date: %s", responseEvent,
			)
			f.updateTransfer("error", msg)
			return &Event{
					Name: "error-in-attempting-apply-path",
					Args: &EventArgs{
						"error": msg,
					},
				},
				backoffState
		}
		err := f.state.alignMountStateWithMasters(clone.Clone.FilesystemId)
		if err != nil {
			return &Event{
				Name: "error-maybe-mounting-filesystem",
				Args: &EventArgs{"error": err, "filesystemId": clone.Clone.FilesystemId},
			}, backoffState
		}
		err = f.incrementPollResultIndex(transferRequestId, pollResult)
		if err != nil {
			return &Event{Name: "error-incrementing-poll-result"},
				backoffState
		}
	}
	return responseEvent, nextState
}

func TransferPollResultFromTransferRequest(
	transferRequestId string,
	transferRequest TransferRequest,
	nodeId string,
	index, total int,
	status string,
) TransferPollResult {
	return TransferPollResult{
		TransferRequestId: transferRequestId,
		Peer:              transferRequest.Peer,
		User:              transferRequest.User,
		ApiKey:            transferRequest.ApiKey,
		Direction:         transferRequest.Direction,

		LocalNamespace:   transferRequest.LocalNamespace,
		LocalName:        transferRequest.LocalName,
		LocalBranchName:  transferRequest.LocalBranchName,
		RemoteNamespace:  transferRequest.RemoteNamespace,
		RemoteName:       transferRequest.RemoteName,
		RemoteBranchName: transferRequest.RemoteBranchName,

		// XXX filesystemId varies over the lifetime of a transferRequestId...
		// this is certainly a hack, and may be problematic. in particular, it
		// may result in different clones being pushed to different hosts, in
		// the case of a multi-host target cluster, possibly...
		FilesystemId:    "",
		InitiatorNodeId: nodeId,
		// XXX re-inventing a wheel here? Maybe we can just use the state
		// "status" fields for this? We're using that already for inter-cluster
		// replication.
		Index:  index,
		Total:  total,
		Status: status,
	}
}
