package types

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
)

type FileChange uint

const (
	FileChangeUnknown FileChange = iota
	FileChangeAdded
	FileChangeModified
	FileChangeRemoved
	FileChangeRenamed
)

func (c FileChange) String() string {
	switch c {
	case FileChangeAdded:
		return "+"
	case FileChangeModified:
		return "M"
	case FileChangeRemoved:
		return "-"
	case FileChangeRenamed:
		return "R"
	default:
		return "unknown"
	}
}

type ZFSFileDiff struct {
	Change   FileChange `json:"change"`
	Filename string     `json:"filename"`
}

func EncodeZFSFileDiff(files []ZFSFileDiff) (string, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&files)
	if err != nil {
		return "", nil
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func DecodeZFSFileDiff(data string) ([]ZFSFileDiff, error) {

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("failed base64 decode step: %s", err)
	}

	var files []ZFSFileDiff
	err = gob.NewDecoder(bytes.NewBuffer(decoded)).Decode(&files)
	return files, err
}

type RPCDiffRequest struct {
	FilesystemID string
}

type RPCDiffResponse struct {
	Files []ZFSFileDiff
}
