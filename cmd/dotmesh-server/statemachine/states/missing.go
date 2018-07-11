package statemachine

func missingState(f *fsMachine) stateFn {
	f.transitionedTo("missing", "waiting")
	log.Printf("entering missing state for %s", f.filesystemId)

	// Are we missing because we're being deleted?
	deleted, err := isFilesystemDeletedInEtcd(f.filesystemId)
	if err != nil {
		log.Printf("Error trying to check for filesystem deletion while in missingState: %s", err)
		return backoffState
	}
	if deleted {
		err := f.state.deleteFilesystem(f.filesystemId)
		if err != nil {
			log.Printf("Error deleting filesystem while in missingState: %s", err)
			return backoffState
		}
		return nil
	}

	if f.attemptReceive() {
		f.transitionedTo("missing", "going to receiving because we found snapshots")
		return receivingState
	}

	newSnapsOnMaster := make(chan interface{})
	f.state.newSnapsOnMaster.Subscribe(f.filesystemId, newSnapsOnMaster)
	defer f.state.newSnapsOnMaster.Unsubscribe(f.filesystemId, newSnapsOnMaster)

	f.transitionedTo("missing", "waiting for snapshots or requests")
	select {
	case _ = <-newSnapsOnMaster:
		f.transitionedTo("missing", "new snapshots found on master")
		return receivingState
	case e := <-f.innerRequests:
		f.transitionedTo("missing", fmt.Sprintf("handling %s", e.Name))
		if e.Name == "delete" {
			// We're in the missing state, so the filesystem
			// theoretically isn't here anyway. But it may be present in
			// some internal caches, so we call deleteFilesystem for
			// thoroughness.
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
		} else if e.Name == "transfer" {
			log.Printf("GOT TRANSFER REQUEST (while missing) %+v", e.Args)

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

			if f.lastTransferRequest.Direction == "push" {
				// Can't push when we're missing.
				f.innerResponses <- &Event{
					Name: "cant-push-while-missing",
					Args: &EventArgs{"request": e, "node": f.state.myNodeId},
				}
				return backoffState
			} else if f.lastTransferRequest.Direction == "pull" {
				return pullInitiatorState
			}
		} else if e.Name == "s3-transfer" {
			log.Printf("GOT S3 TRANSFER REQUEST (while missing) %+v", e.Args)

			// TODO dedupe
			transferRequest, err := s3TransferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &Event{
					Name: "cant-cast-s3-transfer-request",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastS3TransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &Event{
					Name: "cant-cast-s3-transfer-requestid",
					Args: &EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			if f.lastS3TransferRequest.Direction == "push" {
				// Can't push when we're missing.
				f.innerResponses <- &Event{
					Name: "cant-push-while-missing",
					Args: &EventArgs{"request": e, "node": f.state.myNodeId},
				}
				return backoffState
			} else if f.lastS3TransferRequest.Direction == "pull" {
				log.Printf("%s %s %s", ZFS, "create", fq(f.filesystemId))
				out, err := exec.Command(ZFS, "create", fq(f.filesystemId)).CombinedOutput()
				if err != nil {
					log.Printf("%v while trying to create %s", err, fq(f.filesystemId))
					f.innerResponses <- &Event{
						Name: "failed-create",
						Args: &EventArgs{"err": err, "combined-output": string(out)},
					}
					return backoffState
				}
				responseEvent, _ := f.mount()
				if responseEvent.Name == "mounted" {

					return s3PullInitiatorState
				} else {
					f.innerResponses <- responseEvent
					return backoffState
				}
			} else {
				log.Printf("Unknown direction %s, going to backoff", f.lastS3TransferRequest.Direction)
				f.innerResponses <- &Event{
					Name: "failed-s3-transfer",
					Args: &EventArgs{"unknown-direction": f.lastS3TransferRequest.Direction},
				}
				return backoffState
			}
		} else if e.Name == "peer-transfer" {
			// A transfer has been registered. Try to go into the appropriate
			// state.

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

			if f.lastTransferRequest.Direction == "pull" {
				// Can't provide for an initiator trying to pull when we're missing.
				f.innerResponses <- &Event{
					Name: "cant-provide-pull-while-missing",
					Args: &EventArgs{"request": e, "node": f.state.myNodeId},
				}
				return backoffState
			} else if f.lastTransferRequest.Direction == "push" {
				log.Printf("GOT PEER TRANSFER REQUEST FROM MISSING %+v", f.lastTransferRequest)
				return pushPeerState
			}
		} else if e.Name == "create" {
			f.transitionedTo("missing", "creating")
			// ah - we are going to be created on this node, rather than
			// received into from a master...
			log.Printf("%s %s %s", ZFS, "create", fq(f.filesystemId))
			out, err := exec.Command(ZFS, "create", fq(f.filesystemId)).CombinedOutput()
			if err != nil {
				log.Printf("%v while trying to create %s", err, fq(f.filesystemId))
				f.innerResponses <- &Event{
					Name: "failed-create",
					Args: &EventArgs{"err": err, "combined-output": string(out)},
				}
				return backoffState
			}
			responseEvent, nextState := f.mount()
			if responseEvent.Name == "mounted" {
				f.innerResponses <- &Event{Name: "created"}
				return activeState
			} else {
				f.innerResponses <- responseEvent
				return nextState
			}
		} else if e.Name == "mount" {
			f.innerResponses <- &Event{
				Name: "nothing-to-mount",
				Args: &EventArgs{},
			}
			return missingState
		} else {
			f.innerResponses <- &Event{
				Name: "unhandled",
				Args: &EventArgs{"current-state": f.currentState, "event": e},
			}
			log.Printf("unhandled event %s while in missingState", e)
		}
	}
	// something unknown happened, go and check the state of the system after a
	// short timeout to avoid busylooping
	f.transitionedTo("missing", "backing off as we don't know what else to do")
	return backoffState
}
