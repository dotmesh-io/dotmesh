package types

type FileChange int

const (
	FileChangeUnknown FileChange = iota
	FileChangeAdded
	FileChangeModified
	FileChangeRemoved
	FileChangeRenamed
)

type ZFSFileDiff struct {
	Change   FileChange
	Filename string
}

type RPCDiffRequest struct {
	FilesystemID string //
	SnapshotID   string // snapshot (defaults to latest)
}

type RPCDiffResponse struct {
	Files []ZFSFileDiff
}
