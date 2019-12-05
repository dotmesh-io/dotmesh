package fsm

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/archiver"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"

	securejoin "github.com/cyphar/filepath-securejoin"
	log "github.com/sirupsen/logrus"
)

// Given a file's path inside the dot default branch, return its actual path on
// the host filesystem, securely.
func (f *FsMachine) getPathInFilesystem(pathInDot string) (string, error) {
	rootPath := filepath.Join(utils.Mnt(f.filesystemId), "__default__")
	return securejoin.SecureJoin(rootPath, pathInDot)
}

func (f *FsMachine) saveFile(file *types.InputFile) StateFn {
	// create the default paths
	destPath, err := f.getPathInFilesystem(file.Filename)

	l := log.WithFields(log.Fields{
		"filename": file.Filename,
		"destPath": destPath,
	})
	if err != nil {
		e := types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("insecure path, error: %s", err)},
		}
		l.WithError(err).Error("[saveFile] insecure path")
		file.Response <- &e
		return backoffState
	}

	directoryPath := destPath[:strings.LastIndex(destPath, "/")]
	err = os.MkdirAll(directoryPath, 0775)
	if err != nil {
		e := types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to create directory, error: %s", err)},
		}
		l.WithField("directoryPath", directoryPath).WithError(err).Error("[saveFile] Error creating directory")
		file.Response <- &e
		return backoffState
	}

	bytes, err := writeContents(l, file, destPath)
	if err != nil {
		e := types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("cannot to create a file, error: %s", err)},
		}
		l.WithError(err).Error("[saveFile] Error writing file")
		file.Response <- &e
		return backoffState
	}

	response, _ := f.snapshot(&types.Event{Name: "snapshot",
		Args: &types.EventArgs{"metadata": map[string]string{
			"message":      "Uploaded " + file.Filename + " (" + formatBytes(bytes) + ")",
			"author":       file.User,
			"type":         "upload",
			"upload.type":  "S3",
			"upload.file":  file.Filename,
			"upload.bytes": strconv.FormatInt(bytes, 10),
		}}})
	if response.Name != "snapshotted" {
		e := types.Event{
			Name: types.EventNameSaveFailed,
			Args: &types.EventArgs{"err": "file snapshot failed"},
		}
		l.WithFields(log.Fields{
			"responseName": response.Name,
			"responseArgs": fmt.Sprintf("%#v", *(response.Args)),
		}).Error("[saveFile] Error committing")
		file.Response <- &e
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameSaveSuccess,
		// returning snapshot ID
		Args: response.Args,
	}

	return activeState
}

// Delete a file at the given path.
func (f *FsMachine) deleteFile(file *types.InputFile) StateFn {
	// create the default paths
	destPath, err := f.getPathInFilesystem(file.Filename)

	l := log.WithFields(log.Fields{
		"filename": file.Filename,
		"destPath": destPath,
	})

	if err != nil {
		e := types.Event{
			Name: types.EventNameDeleteFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("cannot delete the file, error: %s", err)},
		}
		l.WithError(err).Error("[deleteFile] Error deleting file, insecure path")
		file.Response <- &e
		return backoffState
	}

	_, err = statFile(file.Filename, destPath, file.Response)
	if err != nil {
		l.WithError(err).Error("[deleteFile] Error statting")
		return backoffState
	}
	err = os.RemoveAll(destPath)
	if err != nil {
		e := types.Event{
			Name: types.EventNameDeleteFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("cannot to delete the file, error: %s", err)},
		}
		l.WithError(err).Error("[deleteFile] Error deleting file")
		file.Response <- &e
		return backoffState
	}

	response, _ := f.snapshot(&types.Event{Name: "snapshot",
		Args: &types.EventArgs{"metadata": map[string]string{
			"message":     "Delete " + file.Filename,
			"author":      file.User,
			"type":        "delete",
			"delete.type": "S3",
			"delete.file": file.Filename,
		}}})
	if response.Name != "snapshotted" {
		e := types.Event{
			Name: types.EventNameDeleteFailed,
			Args: &types.EventArgs{"err": "file snapshot failed"},
		}
		l.WithFields(log.Fields{
			"responseName": response.Name,
			"responseArgs": fmt.Sprintf("%#v", *(response.Args)),
		}).Error("[saveFile] Error committing")
		file.Response <- &e
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameDeleteSuccess,
		// returning snapshot ID
		Args: response.Args,
	}

	return activeState
}

func writeContents(l *log.Entry, file *types.InputFile, destinationPath string) (int64, error) {

	if file.Extract {
		return 0, writeAndExtractContents(l, file, destinationPath)
	}

	out, err := os.Create(destinationPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file, error: %s", err)
	}

	defer func() {
		err := out.Close()
		if err != nil {
			l.WithFields(log.Fields{
				"error": err,
			}).Error("s3 saveFile: got error while closing output file")
		}
	}()

	bytes, err := io.Copy(out, file.Contents)
	if err != nil {
		l.WithError(err).Error("[saveFile] Error writing file")
		return 0, fmt.Errorf("cannot to create a file, error: %s", err)
	}

	return bytes, nil
}

// TODO: calculate written bytes if we want it
func writeAndExtractContents(l *log.Entry, file *types.InputFile, destinationPath string) error {
	tDir, err := ioutil.TempDir(os.TempDir(), "s3_dir_upload")
	if err != nil {
		return fmt.Errorf("failed to create temporary dir for unarchiving: %s", err)
	}
	defer os.RemoveAll(tDir)

	archiveFilepath := filepath.Join(tDir, "archive.tar")

	archived, err := os.Create(archiveFilepath)
	if err != nil {
		return fmt.Errorf("failed to create file, error: %s", err)
	}
	_, err = io.Copy(archived, file.Contents)
	if err != nil {
		l.WithError(err).Error("[saveFile] Error writing file")
		return fmt.Errorf("cannot to create a file, error: %s", err)
	}

	// cleaning up destination
	err = os.RemoveAll(destinationPath)
	if err != nil {
		l.WithError(err).Error("failed to clean dir")
		return err
	}

	err = archiver.Unarchive(archiveFilepath, destinationPath)
	if err != nil {
		l.WithError(err).Error("failed to unarchive")
		return err
	}
	return nil
}

func (f *FsMachine) statFile(file *types.OutputFile) StateFn {

	// create the default paths
	sourcePath, err := file.GetFilePath()

	l := log.WithFields(log.Fields{
		"filename":   file.Filename,
		"sourcePath": sourcePath,
	})

	if err != nil {
		file.Response <- &types.Event{
			Name: types.EventNameReadFailed,
			Args: &types.EventArgs{"err": fmt.Errorf("failed to get path %s, error: %s", file.Filename, err)},
		}
		l.WithError(err).Error("[statFile] Error statting, insecure path")
		return backoffState
	}

	fi, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			file.Response <- &types.Event{
				Name: types.EventNameFileNotFound,
				Args: &types.EventArgs{"err": fmt.Errorf("failed to stat %s, error: %s", file.Filename, err)},
			}
		} else {
			file.Response <- &types.Event{
				Name: types.EventNameReadFailed,
				Args: &types.EventArgs{"err": fmt.Errorf("failed to stat %s, error: %s", file.Filename, err)},
			}
		}
		l.WithError(err).Error("[statFile] Error statting")
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameReadSuccess,
		Args: &types.EventArgs{
			"mode": fi.Mode(),
			"size": fi.Size(),
		},
	}

	return activeState
}

// Run Stat() on a file, if error is return then an appropriate event will be
// pushed into the response channel.
func statFile(filename, sourcePath string, response chan *types.Event) (os.FileInfo, error) {
	fi, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			response <- &types.Event{
				Name: types.EventNameFileNotFound,
				Args: &types.EventArgs{"err": fmt.Errorf("failed to stat %s, error: %s", filename, err)},
			}
		} else {
			response <- &types.Event{
				Name: types.EventNameReadFailed,
				Args: &types.EventArgs{"err": fmt.Errorf("failed to stat %s, error: %s", filename, err)},
			}
		}
	}
	return fi, err
}

func (f *FsMachine) readFile(file *types.OutputFile) StateFn {

	// create the default paths
	sourcePath, err := file.GetFilePath()

	l := log.WithFields(log.Fields{
		"filename":   file.Filename,
		"sourcePath": sourcePath,
	})

	if err != nil {
		if err != nil {
			file.Response <- &types.Event{
				Name: types.EventNameReadFailed,
				Args: &types.EventArgs{"err": fmt.Errorf("failed to read file, error: %s", err)},
			}
			l.WithError(err).Error("[readFile] Error opening, insecure path")
			return backoffState
	}

	fi, err := statFile(file.Filename, sourcePath, file.Response)
	if err != nil {
		l.WithError(err).Error("[readFile] Error statting")
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
		l.WithError(err).Error("[readFile] Error opening")
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
		l.WithError(err).Error("[readFile] Error reading")
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

	l := log.WithFields(log.Fields{
		"filename": file.Filename,
		"dirPath":  dirPath,
	})

	stat, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			file.Response <- types.NewErrorEvent(types.EventNameFileNotFound, fmt.Errorf("failed to stat dir '%s', error: %s ", file.Filename, err))
		} else {
			file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("failed to stat dir '%s', error: %s ", file.Filename, err))
		}
		l.WithError(err).Error("[readDirectory] Error statting")
		return backoffState
	}

	if !stat.IsDir() {
		file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("path '%s' is not a directory, error: %s ", file.Filename, err))
		l.WithError(err).Error("[readDirectory] It isn't a directory")
		return backoffState
	}

	err = archiver.NewTar().ArchiveToStream(file.Contents, []string{dirPath})
	if err != nil {
		file.Response <- types.NewErrorEvent(types.EventNameReadFailed, fmt.Errorf("path '%s' tar failed, error: %s ", file.Filename, err))
		l.WithError(err).Error("[readDirectory] Cannot create tar stream")
		return backoffState
	}

	file.Response <- &types.Event{
		Name: types.EventNameReadSuccess,
		Args: &types.EventArgs{},
	}

	return activeState

}
