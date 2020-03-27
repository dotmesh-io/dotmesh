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
