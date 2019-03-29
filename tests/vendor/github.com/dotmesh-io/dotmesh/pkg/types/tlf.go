package types

// special admin user with global privs
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"
const ANONYMOUS_USER_UUID = "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"

type TopLevelFilesystem struct {
	MasterBranch         DotmeshVolume
	OtherBranches        []DotmeshVolume
	Owner                SafeUser
	Collaborators        []SafeUser
	ForkParentId         string
	ForkParentSnapshotId string
}

func (t TopLevelFilesystem) AuthorizeOwner(user *User) (bool, error) {
	return t.authorize(user, false)
}

func (t TopLevelFilesystem) Authorize(user *User) (bool, error) {
	return t.authorize(user, true)
}

func (t TopLevelFilesystem) authorize(user *User, includeCollab bool) (bool, error) {
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
