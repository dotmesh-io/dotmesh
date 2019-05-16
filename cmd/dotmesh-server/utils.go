package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// NB: It's important that the following includes characters _not_ included in
// the base64 alphabet. https://en.wikipedia.org/wiki/Base64
var END_DOTMESH_PRELUDE = types.EndDotmeshPrelude

// apply the instructions encoded in the prelude to the system
func applyPrelude(prelude types.Prelude, fqfs string) error {
	// iterate over it setting zfs user properties accordingly.
	log.Printf("[applyPrelude] Got prelude: %+v", prelude)
	for _, j := range prelude.SnapshotProperties {
		metadataEncoded, err := utils.EncodeMetadata(j.Metadata)
		if err != nil {
			return err
		}
		for _, k := range metadataEncoded {
			// eh, would be better to refactor encodeMetadata
			if k != "-o" {
				args := []string{"set"}
				args = append(args, k)
				args = append(args, fqfs+"@"+j.Id)
				out, err := exec.Command(ZFS, args...).CombinedOutput()
				if err != nil {
					log.Errorf(
						"[applyPrelude] Error applying prelude: %s, %s, %s", args, err, out,
					)
					return fmt.Errorf("Error applying prelude: %s -> %v: %s", args, err, out)
				}
				// log.Debugf("[applyPrelude] Applied snapshot props for: %s", j.Id)
			}
		}
	}
	return nil
}

func toJsonString(value interface{}) string {
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("Error encoding: %+v", err)
	} else {
		return string(bytes)
	}
}

func mnt(fs string) string {
	// from filesystem id to the path it would be mounted at if it were mounted
	mountPrefix := os.Getenv("MOUNT_PREFIX")
	if mountPrefix == "" {
		panic(fmt.Sprintf("Environment variable MOUNT_PREFIX must be set\n"))
	}
	// carefully make this match...
	// MOUNT_PREFIX will be like /dotmesh-test-pools/pool_123_1/mnt
	// and we want to return
	// /dotmesh-test-pools/pool_123_1/mnt/dmfs/:filesystemId
	// fq(fs) gives pool_123_1/dmfs/:filesystemId
	// so don't use it, construct it ourselves:
	return fmt.Sprintf("%s/%s/%s", mountPrefix, ROOT_FS, fs)
}
func unmnt(p string) (string, error) {
	// From mount path to filesystem id
	mountPrefix := os.Getenv("MOUNT_PREFIX")
	if mountPrefix == "" {
		return "", fmt.Errorf("Environment variable MOUNT_PREFIX must be set\n")
	}
	if strings.HasPrefix(p, mountPrefix+"/"+ROOT_FS+"/") {
		return strings.TrimPrefix(p, mountPrefix+"/"+ROOT_FS+"/"), nil
	} else {
		return "", fmt.Errorf("Mount path %s does not start with %s/%s", p, mountPrefix, ROOT_FS)
	}
}

func isFilesystemMounted(fs string) (bool, error) {
	code, err := utils.ReturnCode("mountpoint", mnt(fs))
	if err != nil {
		return false, err
	}
	return code == 0, nil
}

func containerMntParent(id VolumeName) string {
	return CONTAINER_MOUNT_PREFIX + "/" + id.Namespace
}

func containerMnt(id VolumeName) string {
	return containerMntParent(id) + "/" + id.Name
}

func containerMntSubvolume(id VolumeName, subvolume string) string {
	if subvolume != "" {
		return containerMnt(id) + "/" + subvolume
	} else {
		return containerMnt(id)
	}
}

func deleteContainerMntSymlink(id VolumeName) error {
	path := containerMnt(id)
	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Already gone!
			return nil
		} else {
			// Something went wrong :-(
			return err
		}
	}

	// Deletion happened OK
	return nil
}

func getLogfile(logfile string) *os.File {
	if LOG_TO_STDOUT {
		return os.Stdout
	}
	f, err := os.OpenFile(
		fmt.Sprintf("%s.log", logfile),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666,
	)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	return f
}

// setup logfile
func setupLogging() {
	log.SetOutput(getLogfile("dotmesh"))
}

// run forever
func runForever(f func() error, label string, errorBackoff, successBackoff time.Duration) {
	for {
		err := f()
		if err != nil {
			log.Printf("Error in runForever(%s), retrying in %s: %s", label, errorBackoff, err)
			time.Sleep(errorBackoff)
		} else {
			time.Sleep(successBackoff)
		}
	}
}

// var deathObserver = observer.NewObserver("deathObserver")

// run while filesystem lives
// func runWhileFilesystemLives(f func() error, label string, filesystemId string, errorBackoff, successBackoff time.Duration) {
// 	deathChan := make(chan interface{})
// 	deathObserver.Subscribe(filesystemId, deathChan)
// 	stillAlive := true
// 	for stillAlive {
// 		select {
// 		case _ = <-deathChan:
// 			stillAlive = false
// 		default:
// 			err := f()
// 			if err != nil {
// 				log.Printf(
// 					"Error in runWhileFilesystemLives(%s@%s), retrying in %s: %s",
// 					label, filesystemId, errorBackoff, err)
// 				time.Sleep(errorBackoff)
// 			} else {
// 				time.Sleep(successBackoff)
// 			}
// 		}
// 	}
// 	deathObserver.Unsubscribe(filesystemId, deathChan)
// }

// func terminateRunnersWhileFilesystemLived(filesystemId string) {
// 	deathObserver.Publish(filesystemId, struct{ reason string }{"runWhileFilesystemLives"})
// }

func (s *InMemoryState) waitForFilesystemDeath(filesystemId string) {
	// We hold this lock to avoid a race between subscribe and check.

	// Also, the code in fsm.go that calls
	// terminateRUnnersWhileFilesystemLived sends the message to
	// deathObserver *after* removing the filesystem from the map, so
	// waitForFilesystemDeath will always return if its execution is
	// overlapping with the execition of the termination code in
	// FsMachine.Run.
	var returnImmediately bool
	deathChan := make(chan interface{})
	func() {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		fs, ok := s.filesystems[filesystemId]
		if ok {
			log.Printf("[waitForFilesystemDeath:%s] state: %s, status: %s", filesystemId, fs.GetCurrentState(), fs.GetStatus())
		} else {
			log.Printf("[waitForFilesystemDeath:%s] no fsMachine", filesystemId)
		}
		if !ok {
			// filesystem is already gone!
			returnImmediately = true
			return
		}
		s.deathObserver.Subscribe(filesystemId, deathChan)
	}()
	if !returnImmediately {
		<-deathChan
		s.deathObserver.Unsubscribe(filesystemId, deathChan)
	}
}

// From http://marcio.io/2015/07/singleton-pattern-in-go/

// Once is an object that will perform exactly one action.
type Once struct {
	m    sync.Mutex
	done uint32
}

// Do calls the function f if and only if Do is being called for the
// first time for this instance of Once. In other words, given
//	var once Once
// if once.Do(f) is called multiple times, only the first call will invoke f,
// even if f has a different value in each invocation.  A new instance of
// Once is required for each function to execute.
//
// Do is intended for initialization that must be run exactly once.  Since f
// is niladic, it may be necessary to use a function literal to capture the
// arguments to a function to be invoked by Do:
//	config.once.Do(func() { config.init(filename) })
//
// Because no call to Do returns until the one call to f returns, if f causes
// Do to be called, it will deadlock.
//
// If f panics, Do considers it to have returned; future calls of Do return
// without calling f.
//
func (o *Once) Do(f func()) {
	if atomic.LoadUint32(&o.done) == 1 { // <-- Check
		return
	}
	// Slow-path.
	o.m.Lock() // <-- Lock
	defer o.m.Unlock()
	if o.done == 0 { // <-- Check
		defer atomic.StoreUint32(&o.done, 1)
		f()
	}
}

func restrictSnapshots(localSnaps []*Snapshot, toSnapshotId string) ([]*Snapshot, error) {
	if toSnapshotId != "" {
		newLocalSnaps := []*Snapshot{}
		for _, s := range localSnaps {
			newLocalSnaps = append(newLocalSnaps, s)
			if s.Id == toSnapshotId {
				return newLocalSnaps, nil
			}
		}
		return newLocalSnaps, fmt.Errorf("Unable to find %s in %+v", toSnapshotId, localSnaps)
	}
	return localSnaps, nil
}

// FIXME: Put this in a shared library, as it duplicates the copy in dm/pkg/remotes/api.go
func parseNamespacedVolume(name string) (string, string, error) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 0: // name was empty
		return "", "", nil
	case 1: // name was unqualified, no namespace, so we default to "admin"
		return "admin", name, nil
	case 2: // Qualified name
		return parts[0], parts[1], nil
	default: // Too many slashes!
		return "", "", fmt.Errorf("Volume names must be of the form NAMESPACE/VOLUME or just VOLUME: '%s'", name)
	}
}

func parseNamespacedVolumeWithSubvolumes(name string) (string, string, string, error) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 0: // name was empty
		return "", "", "", nil
	case 1: // volume with no subvolume
		namespace, name, err := parseNamespacedVolume(parts[0])
		if err != nil {
			return "", "", "", err
		}
		return namespace, name, "__default__", nil
	case 2: // volume with subvolume
		namespace, name, err := parseNamespacedVolume(parts[0])
		if err != nil {
			return "", "", "", err
		}
		if strings.ContainsAny(parts[1], "$:/.@") {
			return "", "", "", fmt.Errorf("Subdot names must not contain $, :, /, ., or @: '%s'", name)
		}
		if parts[1] == "__root__" {
			return namespace, name, "", nil
		} else {
			return namespace, name, parts[1], nil
		}
	default: // Too many colons!
		return "", "", "", fmt.Errorf("Volume names must be of the form [NAMESPACE/]DOT[.SUBDOT]: '%s'", name)
	}
}

func Copy(src, dst string, fileMode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	err = out.Chmod(fileMode)
	if err != nil {
		return err
	}

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

// TODO dedupe this wrt testtools
func tryUntilSucceeds(f func() error, desc string) error {
	return tryUntilSucceedsN(f, desc, 5)
}

func tryUntilSucceedsN(f func() error, desc string, retries int) error {
	attempt := 0
	for {
		err := f()
		if err != nil {
			if attempt > retries {
				return err
			} else {
				fmt.Printf("Error %s: %v, pausing and trying again...\n", desc, err)
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		} else {
			return nil
		}
		attempt++
	}
}

// we are removing lots of logging but let's enable them to be switched back on
// if the PRINT_QUIET_LOGS env is not empty
func quietLogger(logMessage string) {
	if os.Getenv("PRINT_QUIET_LOGS") != "" {
		log.Printf(logMessage)
	}
}

func getMyStack() string {
	len := 1024
	for {
		buf := make([]byte, len)
		used := runtime.Stack(buf, false)
		if used < len {
			return string(buf[:used])
		} else {
			len = len * 2
		}
	}
}

func formatBytes(i int64) string {
	var f float64 = float64(i)
	switch {
	case i < 0:
		return "-" + formatBytes(-i)
	case i >= 1024*1024*1024*1024:
		return fmt.Sprintf("%.1fTiB", f/(1024.0*1024.0*1024.0*1024.0))
	case i >= 1024*1024*1024:
		return fmt.Sprintf("%.1fGiB", f/(1024.0*1024.0*1024.0))
	case i >= 1024*1024:
		return fmt.Sprintf("%.1fMiB", f/(1024.0*1024.0))
	case i >= 1024:
		return fmt.Sprintf("%.1fKiB", f/1024.0)
	default:
		return fmt.Sprintf("%dB", i)
	}
}
