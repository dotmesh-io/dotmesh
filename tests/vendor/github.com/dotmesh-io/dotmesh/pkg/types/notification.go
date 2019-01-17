package types

// CommitNotification - is used by dotmesh server to send notifications
// about new commits
type CommitNotification struct {
	FilesystemId string
	Namespace    string
	Name         string
	Branch       string
	CommitId     string
	Metadata     map[string]string

	OwnerID         string
	CollaboratorIDs []string
}

// NATSPublishCommitsSubject - default NATS subject when sending commit
// notifications
const NATSPublishCommitsSubject = "dotmesh.commits"
