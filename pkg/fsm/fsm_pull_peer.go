package fsm

import "github.com/dotmesh-io/dotmesh/pkg/types"

func pullPeerState(f *FsMachine) StateFn {
	// This is kind-of a boring state. An authenticated user can GET a
	// filesystem whenever. So arguably a valid implementation of pullPeerState
	// is just to immediately go back to discoveringState. In the future, it
	// might be nicer for observability to synchronize staying in this state
	// until our peer has what it needs. And maybe we want to block some other
	// events while this is happening? (Although I think we want to do that for
	// GETs in general?)

	f.transitionedTo("pullPeerState", "cleaning-tmp")

	// Clear out any tmp diff snapshot that exists on the fs to avoid
	// accidentally sending it somewhere.
	err := f.zfs.DestroyTmpSnapIfExists(f.filesystemId)
	if err != nil {
		f.innerResponses <- &types.Event{
			Name: "cant-destroy-tmp-snap-if-exists",
			Args: &types.EventArgs{"err": err},
		}
		return backoffState
	}

	f.transitionedTo("pullPeerState", "immediate-return")
	f.innerResponses <- &types.Event{
		Name: "awaiting-transfer",
		Args: &types.EventArgs{},
	}
	return discoveringState
}
