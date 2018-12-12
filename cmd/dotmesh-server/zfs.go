package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/metrics"
	"github.com/dotmesh-io/dotmesh/pkg/zfs"
)

// functions which relate to interacting directly with zfs

// Log a ZFS command was run. This is used to recreate ZFS states when
// investigating bugs.

func logZFSCommand(filesystemId, command string) {
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

func findLocalPoolId() (string, error) {
	output, err := exec.Command(ZPOOL, "get", "-H", "guid", POOL).CombinedOutput()
	if err != nil {
		return string(output), err
	}
	i, err := strconv.ParseUint(strings.Split(string(output), "\t")[2], 10, 64)
	if err != nil {
		return string(output), err
	}
	return fmt.Sprintf("%x", i), nil
}

func (state *InMemoryState) reportZpoolCapacity() error {
	capacity, err := zfs.GetZPoolCapacity(state.config.ZPoolPath, state.config.PoolName)
	if err != nil {
		return err
	}
	metrics.ZPoolCapacity.WithLabelValues(state.myNodeId, state.config.PoolName).Set(capacity)
	return nil
}

func (s *InMemoryState) findFilesystemIdsOnSystem() []string {
	// synchronously, return slice of filesystem ids that exist.
	log.Print("Finding filesystem ids...")
	listArgs := []string{"list", "-H", "-r", "-o", "name", POOL + "/" + ROOT_FS}
	// look before you leap (check error code of zfs list)
	code, err := returnCode(s.config.ZFSExecPath, listArgs...)
	if err != nil {
		log.Fatalf("%s, when running zfs list", err)
	}
	// creates pool/dmfs on demand if it doesn't exist.
	if code != 0 {
		output, err := exec.Command(
			ZFS, "create", "-o", "mountpoint=legacy", POOL+"/"+ROOT_FS).CombinedOutput()
		if err != nil {
			out("Unable to create", POOL+"/"+ROOT_FS, "- does ZFS pool '"+POOL+"' exist?\n")
			log.Printf(string(output))
			log.Fatal(err)
		}
	}
	// get output
	output, err := exec.Command(ZFS, listArgs...).Output()
	if err != nil {
		log.Fatalf("%s, while getting output from zfs list", err)
	}
	// output should now contain newline delimited list of fq filesystem names.
	newLines := []string{}
	lines := strings.Split(string(output), "\n")
	// strip off the first one, which is always the root pool itself, and last
	// one which is empty newline
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		newLines = append(newLines, zfs.UnFQ(s.config.PoolName, line))
	}
	return newLines
}

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

func (s *InMemoryState) deleteFilesystemInZFS(fs string) error {
	logZFSCommand(fs, fmt.Sprintf("%s destroy -r %s", s.config.ZFSExecPath, zfs.FQ(s.config.PoolName, fs)))
	cmd := exec.Command(s.config.ZFSExecPath, "destroy", "-r", zfs.FQ(s.config.PoolName, fs))
	err := doSimpleZFSCommand(cmd, fmt.Sprintf("delete filesystem %s (full name: %s)", fs, zfs.FQ(s.config.PoolName, fs)))
	return err
}
