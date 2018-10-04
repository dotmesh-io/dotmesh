package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const eventNameSaveFailed = "save-failed"
const eventNameSaveSuccess = "save-success"

func (f *fsMachine) saveFile(file *File) stateFn {
	// create the default paths
	destPath := fmt.Sprintf("%s/%s/%s", mnt(f.filesystemId), "__default__", file.Filename)
	log.Printf("Saving file to %s", destPath)
	directoryPath := destPath[:strings.LastIndex(destPath, "/")]
	err := os.MkdirAll(directoryPath, 0775)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": fmt.Errorf("failed to create directory, error: %s", err)},
		}
		return backoffState
	}
	out, err := os.Create(destPath)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": fmt.Errorf("failed to create file, error: %s", err)},
		}
		return backoffState
	}

	_, err = io.Copy(out, file.Contents)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": fmt.Errorf("cannot to create a file, error: %s", err)},
		}
		return backoffState
	}
	err = out.Close()
	if err != nil {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": fmt.Errorf("cannot close the file, error: %s", err)},
		}
		return backoffState
	}
	response, _ := f.snapshot(&Event{Name: "snapshot",
		Args: &EventArgs{"metadata": metadata{"message": "saving file put by s3 api " + file.Filename, "author": file.User}}})
	if response.Name != "snapshotted" {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": "file snapshot failed"},
		}
		return backoffState
	}

	file.Response <- &Event{
		Name: eventNameSaveSuccess,
		Args: &EventArgs{},
	}

	return activeState
}
