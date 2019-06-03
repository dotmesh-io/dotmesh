package zfs

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
)

func doSimpleZFSCommand(cmd *exec.Cmd, description string) error {
	errBuffer := bytes.Buffer{}
	cmd.Stderr = &errBuffer
	err := cmd.Run()
	if err != nil {
		readBytes, readErr := ioutil.ReadAll(&errBuffer)
		if readErr != nil {
			return fmt.Errorf("error reading error: %v", readErr)
		}
		return fmt.Errorf("error running ZFS command to %s: %v / %v", description, err, string(readBytes))
	}

	return nil
}

// zfsCommandWithRetries runs a given command, it will retry as long as ctx is not cancelled.
func zfsCommandWithRetries(ctx context.Context, description, name string, arg ...string) error {
	var err error
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("deadline exceeded, last error: %s", err)
		default:
		}
		cmd := exec.Command(name, arg...)
		errBuffer := bytes.Buffer{}
		cmd.Stderr = &errBuffer
		cmdErr := cmd.Run()
		if cmdErr != nil {
			readBytes, readErr := ioutil.ReadAll(&errBuffer)
			if readErr != nil {
				err = fmt.Errorf("error reading error: %v", readErr)

				log.WithFields(log.Fields{
					"error":       err,
					"description": description,
				}).Warn("[zfsCommandWithRetries] failed to read error buffer after command failed")

				time.Sleep(500 * time.Millisecond)
				continue
			}

			err = fmt.Errorf("error running ZFS command to %s: %v / %v", description, cmdErr, string(readBytes))

			log.WithFields(log.Fields{
				"error":       err,
				"description": description,
			}).Warn("[zfsCommandWithRetries] zfs command failed")

			time.Sleep(500 * time.Millisecond)
			continue
		}

		// success!
		return nil

	}
}

// TODO: why not use a logger and using a file with it
// then tune the level? This is used aalllll over the codebase.
func LogZFSCommand(filesystemId, command string) {
	// Disabled by default; we need to change the code and recompile to enable this.
	if false {
		f, err := os.OpenFile(os.Getenv("POOL_LOGFILE"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}

		defer f.Close()

		fmt.Fprintf(f, "%s # %s\n", command, filesystemId)
	}
}
