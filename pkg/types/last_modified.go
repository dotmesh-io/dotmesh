package types

import "time"

type LastModified struct {
	Time time.Time
}

type RPCLastModifiedRequest struct {
	FilesystemID string
}
