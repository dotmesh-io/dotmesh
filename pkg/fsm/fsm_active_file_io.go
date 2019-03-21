package fsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/archiver"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func (f *FsMachine) saveFile(file *types.InputFile) StateFn {
	// create the default paths
	destPath := fmt.Sprintf("%s/%s/%s", utils.Mnt(f.filesystemId), "__default__", file.Filename)
	log.Printf("Saving file to %s", destPath)
	directoryPath := destPath[:strings.LastIndex(destPath, "/")]
	err := os.MkdirAll(directoryPath, 0775)
	if err != nil {
		file.Response <- &types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to create directory, error: %s", err)},
		}
		return backoffState
	}
	out, err := os.Create(destPath)
	if err != nil {
		file.Response <- &types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to create file, error: %s", err)},
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
		file.Response <- &types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("cannot to create a file, error: %s", err)},
		}
		return backoffState
	}
	response, _ := f.snapshot(&types.Event{Name: "snapshot",
		Args: &types.EventArgs{"metadata": map[string]string{
			"message":      "Uploaded " + file.Filename + " (" + formatBytes(bytes) + ")",
			"author":       file.User,
			"type":         "upload",
			"upload.type":  "S3",
			"upload.file":  file.Filename,
			"upload.bytes": fmt.Sprintf("%d", bytes),
		}}})
	if response.Name != "snapshotted" {
		file.Response <- &types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": "file snapshot failed"},
		}
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameSaveSuccess,
		Args: &types.EventArgs{},
	}

	return activeState
}

func (f *FsMachine) readFile(file *types.OutputFile) StateFn {

	// create the default paths
	sourcePath := fmt.Sprintf("%s/%s/%s", file.SnapshotMountPath, "__default__", file.Filename)

	fi, err := os.Stat(sourcePath)
	if err != nil {
		file.Response <- &types.Event{
			Name: types.EventNameReadFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to stat %s, error: %s", file.Filename, err)},
		}
		return backoffState
	}

	if fi.IsDir() {
		return f.readDirectory(file)
	}

	fileOnDisk, err := os.Open(sourcePath)
	if err != nil {
		file.Response <- &types.Event{
			Name: types.EventNameReadFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to read file, error: %s", err)},
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
		file.Response <- &types.Event{
			Name: types.EventNameReadFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("cannot stream file, error: %s", err)},
		}
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameReadSuccess,
		Args: &types.EventArgs{},
	}

	return activeState
}

func (f *FsMachine) readDirectory(file *types.OutputFile) StateFn {

	dirPath := filepath.Join(file.SnapshotMountPath, "__default__", file.Filename)

	stat, err := os.Stat(dirPath)
	if err != nil {
		file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("failed to stat dir '%s', error: %s ", file.Filename, err))
		return backoffState
	}

	if !stat.IsDir() {
		file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("path '%s' is not a directory, error: %s ", file.Filename, err))
		return backoffState
	}

	err = archiver.NewTar().ArchiveToStream(file.Contents, []string{dirPath})
	if err != nil {
		file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("path '%s' tar failed, error: %s ", file.Filename, err))
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameReadSuccess,
		Args: &types.EventArgs{},
	}

	return activeState

}
