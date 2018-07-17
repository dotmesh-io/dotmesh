package types

import (
	"context"
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

// special admin user with global privs
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"

type TopLevelFilesystem struct {
	MasterBranch  DotmeshVolume
	OtherBranches []DotmeshVolume
	Owner         user.SafeUser
	Collaborators []user.SafeUser
}

func (t TopLevelFilesystem) AuthorizeOwner(ctx context.Context) (bool, error) {
	return t.authorize(ctx, false)
}

func (t TopLevelFilesystem) Authorize(ctx context.Context) (bool, error) {
	return t.authorize(ctx, true)
}

func (t TopLevelFilesystem) authorize(ctx context.Context, includeCollab bool) (bool, error) {
	user := auth.GetUserFromCtx(ctx)
	if user == nil {
		return false, fmt.Errorf("No user found in context.")
	}
	// admin user is always authorized (e.g. docker daemon). users and auth are
	// only really meaningful over the network for data synchronization, when a
	// dotmesh cluster is being used like a hub.
	if user.Id == ADMIN_USER_UUID {
		return true, nil
	}
	if user.Id == t.Owner.Id {
		return true, nil
	}
	if includeCollab {
		for _, other := range t.Collaborators {
			if user.Id == other.Id {
				return true, nil
			}
		}
	}
	return false, nil
}
