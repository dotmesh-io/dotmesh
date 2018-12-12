package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"

	log "github.com/sirupsen/logrus"
)

// NB: It's important that the following includes characters _not_ included in
// the base64 alphabet. https://en.wikipedia.org/wiki/Base64
var END_DOTMESH_PRELUDE = types.EndDotmeshPrelude

func consumePrelude(r io.Reader) (Prelude, error) {
	// called when we know that there's a prelude to read from r.

	// read a byte at a time, so that we leave the reader ready for someone
	// else.
	b := make([]byte, 1)
	finished := false
	buf := []byte{}

	for !finished {
		_, err := r.Read(b)
		if err == io.EOF {
			return Prelude{}, fmt.Errorf("Stream ended before prelude completed")
		}
		if err != nil {
			return Prelude{}, err
		}
		buf = append(buf, b...)
		idx := bytes.Index(buf, END_DOTMESH_PRELUDE)
		if idx != -1 {
			preludeEncoded := buf[0:idx]
			data, err := base64.StdEncoding.DecodeString(string(preludeEncoded))
			if err != nil {
				return Prelude{}, err
			}
			p := Prelude{}
			err = json.Unmarshal(data, &p)
			if err != nil {
				return Prelude{}, err
			}
			return p, nil
		}
	}
	return Prelude{}, nil
}

// apply the instructions encoded in the prelude to the system
func applyPrelude(prelude Prelude, fqfs string) error {
	// iterate over it setting zfs user properties accordingly.
	log.Printf("[applyPrelude] Got prelude: %+v", prelude)
	for _, j := range prelude.SnapshotProperties {
		metadataEncoded, err := encodeMetadata(j.Metadata)
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
				log.Debugf("[applyPrelude] Applied snapshot props for: %s", j.Id)
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

func encodePrelude(prelude Prelude) ([]byte, error) {
	// encode a prelude as JSON wrapped up in base64. The reason for the base64
	// is to avoid framing issues. This works because END_DOTMESH_PRELUDE has
	// non-base64 characters in it.
	preludeBytes, err := json.Marshal(prelude)
	if err != nil {
		return []byte{}, err
	}
	encoded := []byte(base64.StdEncoding.EncodeToString(preludeBytes))
	encoded = append(encoded, END_DOTMESH_PRELUDE...)
	return encoded, nil
}

// utility functions
func out(s ...interface{}) {
	stringified := []string{}
	for _, item := range s {
		stringified = append(stringified, fmt.Sprintf("%v", item))
	}
	ss := strings.Join(stringified, " ")
	os.Stdout.Write([]byte(ss))
}

func fq(fs string) string {
	// from filesystem id to a fully qualified ZFS filesystem
	return fmt.Sprintf("%s/%s/%s", POOL, ROOT_FS, fs)
}
func unfq(fqfs string) string {
	// from fully qualified ZFS name to filesystem id, strip off prefix
	return fqfs[len(POOL+"/"+ROOT_FS+"/"):]
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
	code, err := returnCode("mountpoint", mnt(fs))
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

func returnCode(name string, arg ...string) (int, error) {
	// Run a command and either get the returncode or an error if the command
	// failed to execute, based on
	// http://stackoverflow.com/questions/10385551/get-exit-code-go
	cmd := exec.Command(name, arg...)
	if err := cmd.Start(); err != nil {
		return -1, err
	}
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0
			// This works on both Unix and Windows. Although package
			// syscall is generally platform dependent, WaitStatus is
			// defined for both Unix and Windows and in both cases has
			// an ExitStatus() method with the same signature.
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				return status.ExitStatus(), nil
			}
		} else {
			return -1, err
		}
	}
	// got here, so err == nil
	return 0, nil
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

// general purpose function, intended to be runnable in a goroutine, which
// reads bytes from a Reader and writes them to a Writer, closing the Writer
// when the Reader yields EOF. should be useable both to pipe command outputs
// into http responses, as well as piping http requests into command inputs.
//
// when EOF is read from the Reader, it writes true into the finished chan to
// notify of completion.
//
// it also performs non-blocking reads on the canceller channel during the
// loop, and aborts reading, closing both Reader and Writer in that case.
// when cancellation happens, cancelFunc is run with the object that was read
// from the canceller chan, in case it needs to be reused. we assume that
// Events flow over the canceller chan.
//
// if the writer implements http.Flusher, Flush() is called after each write.

// TODO: pipe would be better named Copy

func pipe(
	r io.Reader, rDesc string, w io.Writer, wDesc string,
	finished chan bool, canceller chan *Event,
	cancelFunc func(*Event, chan *Event),
	notifyFunc func(int64, int64),
	compressMode string,
) {
	startTime := time.Now().UnixNano()
	var lastUpdate int64 // in UnixNano
	var totalBytes int64
	buffer := make([]byte, types.BufLength)

	// Incomplete idea below.
	/*
		// async buffer e.g. let the network read up to 32MiB of data that's
		// already been written to the buffer without blocking the buffer, or let
		// zfs write 32MiB of data that hasn't been read yet without stalling it
		nioBufOut := niobuffer.New(1024 * 1024 * 1024 * 32)
		bufROut, w := nio.Pipe(nioBufOut)
		go func() {
			nio.Copy(originalW, bufROut, nioBufOut)
		}()
		nioBufIn := niobuffer.New(1024 * 1024 * 1024 * 32)
		r, bufWIn := nio.Pipe(nioBufIn)
		go func() {
			nio.Copy(bufWIn, originalR, nioBufIn)
		}()
	*/

	// only call f() if 1 sec of nanosecs elapsed since last call to f()
	rateLimit := func(f func()) {
		if time.Now().UnixNano()-lastUpdate > 1e+9 {
			f()
			lastUpdate = time.Now().UnixNano()
		}
	}

	handleErr := func(message string, r io.Reader, w io.Writer, r2 io.Reader, w2 io.Writer) {
		if message != "" {
			log.Printf("[pipe:handleErr] " + message)
		}
		// NB: c.Close returns unhandled err here, and below.
		if c, ok := r.(io.Closer); ok {
			c.Close()
		}
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
		if c, ok := r2.(io.Closer); ok {
			c.Close()
		}
		if c, ok := w2.(io.Closer); ok {
			c.Close()
		}
		finished <- true
	}

	var writer io.Writer
	var reader io.Reader
	var err error

	log.Printf("[PIPE] reader %s => writer %s, COMPRESSMODE=%s", rDesc, wDesc, compressMode)

	if compressMode == "compress" {
		writer = gzip.NewWriter(w)
		reader = r
	} else if compressMode == "decompress" {
		reader, err = gzip.NewReader(r)
		if err != nil {
			handleErr(fmt.Sprintf("Unable to create gzip reader: %s", err), r, w, r, w)
			return
		}
		writer = w
	} else if compressMode == "none" {
		// no compression
		reader = r
		writer = w
	} else {
		handleErr(
			fmt.Sprintf(
				"Unsupported compression mode %s, choose one of 'compress', "+
					"'decompress' or 'none'",
				compressMode,
			), r, w, r, w,
		)
		return
	}

	for {
		select {
		case e := <-canceller:
			// call the cancellation function asynchronously, because it may
			// block, and we don't want to deadlock
			go cancelFunc(e, canceller)
			handleErr(
				fmt.Sprintf("Cancelling pipe from %s to %s because %s event "+
					"received on cancellation channel", rDesc, wDesc, e),
				reader, writer, r, w,
			)
			return
		default:
			// non-blocking read
		}
		nr, err := reader.Read(buffer)
		if nr > 0 {
			data := buffer[0:nr]
			nw, wErr := writer.Write(data)
			if nw != nr {
				handleErr(fmt.Sprintf("short write %d (read) != %d (written)", nr, nw), reader, writer, r, w)
				return
			}
			if f, ok := writer.(http.Flusher); ok {
				f.Flush()
			}
			if f, ok := writer.(*gzip.Writer); ok {
				// special case, we know we might have to flush the writer in
				// case of a small replication stream (and we're not speaking
				// directly to an http.Flusher any more)
				f.Flush()
			}
			totalBytes += int64(nr)
			rateLimit(func() {
				// rate limit to once per second to avoid hammering notifyFunc
				// on fast connections.
				notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			})
			if wErr != nil {
				handleErr(fmt.Sprintf("Error writing to %s: %s", wDesc, wErr), reader, writer, r, w)
				return
			}
		}
		// NB: handleErr as the final thing we do in both of the following
		// cases because it's polite to stop notifying (notifyFunc) after we're
		// said we're finished (handleErr).

		if err == io.EOF {
			// sync notification here (and in error case below) in case the
			// caller depends on synchronous notification of final state before
			// exit
			notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			// expected case, log no error
			handleErr("", reader, writer, r, w)
			return
		} else if err != nil {
			notifyFunc(totalBytes, time.Now().UnixNano()-startTime)
			handleErr(fmt.Sprintf("Error reading from %s: %s", rDesc, err), reader, writer, r, w)
			return
		}
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
