package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

const eventNameSaveFailed = "save-failed"
const eventNameSaveSuccess = "save-success"

const eventNameReadFailed = "read-failed"
const eventNameReadSuccess = "read-success"

func (f *fsMachine) saveFile(file *InputFile) stateFn {
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

	defer func() {
		err := out.Close()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"file":  destPath,
			}).Error("s3 saveFile: got error while closing output file")
		}
	}()

	bytes, err := io.Copy(out, file.Contents)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameSaveFailed,
			Args: &EventArgs{"err": fmt.Errorf("cannot to create a file, error: %s", err)},
		}
		return backoffState
	}
	response, _ := f.snapshot(&Event{Name: "snapshot",
		Args: &EventArgs{"metadata": metadata{
			"message":      "Uploaded " + file.Filename + " (" + formatBytes(bytes) + ")",
			"author":       file.User,
			"type":         "upload",
			"upload.type":  "S3",
			"upload.file":  file.Filename,
			"upload.bytes": fmt.Sprintf("%d", bytes),
		}}})
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

func (f *fsMachine) readFile(file *OutputFile) stateFn {
	// create the default paths
	sourcePath := fmt.Sprintf("%s/%s/%s", file.SnapshotMountPath, "__default__", file.Filename)

	fileOnDisk, err := os.Open(sourcePath)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameReadFailed,
			Args: &EventArgs{"err": fmt.Errorf("failed to read file, error: %s", err)},
		}
		return backoffState
	}
	defer func() {
		err := fileOnDisk.Close()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"file":  sourcePath,
			}).Error("s3 readFile: got error while closing file")
		}
	}()
	_, err = io.Copy(file.Contents, fileOnDisk)
	if err != nil {
		file.Response <- &Event{
			Name: eventNameReadFailed,
			Args: &EventArgs{"err": fmt.Errorf("cannot stream file, error: %s", err)},
		}
		return backoffState
	}

	file.Response <- &Event{
		Name: eventNameReadSuccess,
		Args: &EventArgs{},
	}

	return activeState
}
