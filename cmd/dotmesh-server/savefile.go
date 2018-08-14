package main

import (
	"fmt"
	"os"
	"strings"
)

// not really sure where this should live, but this allows us to handle requests the s3 endpoint to save files.
func (f *fsMachine) saveFile(filename string, contents []byte) stateFn {

	// create the default paths
	destPath := fmt.Sprintf("%s/%s/%s", mnt(f.filesystemId), "__default__", filename)
	directoryPath := destPath[:strings.LastIndex(destPath, "/")]
	err := os.MkdirAll(directoryPath, 0775)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-create-dir",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	file, err := os.Create(destPath)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-create-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	_, err = file.Write(contents)
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-write-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	err = file.Close()
	if err != nil {
		f.innerResponses <- &Event{
			Name: "cannot-close-file",
			Args: &EventArgs{"err": err},
		}
		return backoffState
	}
	return discoveringState
}
