package fsm

import (
	"fmt"
	"log"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
)

func (f *FsMachine) isFilesystemDeletedInEtcd(fsId string) (bool, error) {
	_, err := f.filesystemStore.GetDeleted(fsId)
	if err != nil {
		if store.IsKeyNotFound(err) {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

func missingState(f *FsMachine) StateFn {
	f.transitionedTo("missing", "waiting")
	log.Printf("entering missing state for %s", f.filesystemId)

	// Are we missing because we're being deleted?
	deleted, err := f.isFilesystemDeletedInEtcd(f.filesystemId)
	if err != nil {
		log.Printf("Error trying to check for filesystem deletion while in missingState: %s", err)
		return backoffState
	}
	if deleted {
		err := f.state.DeleteFilesystem(f.filesystemId)
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
	f.newSnapsOnMaster.Subscribe(f.filesystemId, newSnapsOnMaster)
	defer f.newSnapsOnMaster.Unsubscribe(f.filesystemId, newSnapsOnMaster)

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
			return nil
		} else if e.Name == "transfer" {
			log.Printf("GOT TRANSFER REQUEST (while missing) %+v", e.Args)

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			if f.lastTransferRequest.Direction == "push" {
				// Can't push when we're missing.
				f.innerResponses <- &types.Event{
					Name: "cant-push-while-missing",
					Args: &types.EventArgs{"request": e, "node": f.state.NodeID()},
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
				f.innerResponses <- &types.Event{
					Name: "cant-cast-s3-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastS3TransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-s3-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			if f.lastS3TransferRequest.Direction == "push" {
				// Can't push when we're missing.
				f.innerResponses <- &types.Event{
					Name: "cant-push-while-missing",
					Args: &types.EventArgs{"request": e, "node": f.state.NodeID()},
				}
				return backoffState
			} else if f.lastS3TransferRequest.Direction == "pull" {
				output, err := f.zfs.Create(f.filesystemId)
				if err != nil {
					f.innerResponses <- &types.Event{
						Name: "failed-create",
						Args: &types.EventArgs{"err": err, "combined-output": string(output)},
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
				f.innerResponses <- &types.Event{
					Name: "failed-s3-transfer",
					Args: &types.EventArgs{"unknown-direction": f.lastS3TransferRequest.Direction},
				}
				return backoffState
			}
		} else if e.Name == "peer-transfer" {
			// A transfer has been registered. Try to go into the appropriate
			// state.

			// TODO dedupe
			transferRequest, err := transferRequestify((*e.Args)["Transfer"])
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-request",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequest = transferRequest
			transferRequestId, ok := (*e.Args)["RequestId"].(string)
			if !ok {
				f.innerResponses <- &types.Event{
					Name: "cant-cast-transfer-requestid",
					Args: &types.EventArgs{"err": err},
				}
				return backoffState
			}
			f.lastTransferRequestId = transferRequestId

			if f.lastTransferRequest.Direction == "pull" {
				// Can't provide for an initiator trying to pull when we're missing.
				f.innerResponses <- &types.Event{
					Name: "cant-provide-pull-while-missing",
					Args: &types.EventArgs{"request": e, "node": f.state.NodeID()},
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
			output, err := f.zfs.Create(f.filesystemId)
			if err != nil {
				f.innerResponses <- &types.Event{
					Name: "failed-create",
					Args: &types.EventArgs{"err": err, "combined-output": string(output)},
				}
				return backoffState
			}
			responseEvent, nextState := f.mount()
			if responseEvent.Name == "mounted" {
				subvolPath := fmt.Sprintf("%s/__default__", utils.Mnt(f.filesystemId))
				if err := os.MkdirAll(subvolPath, 0777); err != nil {
					f.innerResponses <- &types.Event{
						Name: "failed-create-default-subdot",
						Args: &types.EventArgs{"err": err},
					}
					return backoffState
				} else {
					f.innerResponses <- &types.Event{Name: "created"}
					return activeState
				}
			} else {
				f.innerResponses <- responseEvent
				return nextState
			}

		} else if e.Name == "mount" {
			f.innerResponses <- &types.Event{
				Name: "nothing-to-mount",
				Args: &types.EventArgs{},
			}
			return missingState
		} else {
			f.innerResponses <- &types.Event{
				Name: "unhandled",
				Args: &types.EventArgs{"current-state": f.currentState, "event": e},
			}
			log.Printf("unhandled event %s while in missingState", e)
		}
	}
	// something unknown happened, go and check the state of the system after a
	// short timeout to avoid busylooping
	f.transitionedTo("missing", "backing off as we don't know what else to do")
	return backoffState
}
