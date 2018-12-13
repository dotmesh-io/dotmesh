package fsm

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

// TODO: remove that environment getter and replace with parameter
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
	return fmt.Sprintf("%s/%s/%s", mountPrefix, types.RootFS, fs)
}

func unmnt(p string) (string, error) {
	// From mount path to filesystem id
	mountPrefix := os.Getenv("MOUNT_PREFIX")
	if mountPrefix == "" {
		return "", fmt.Errorf("Environment variable MOUNT_PREFIX must be set\n")
	}
	if strings.HasPrefix(p, mountPrefix+"/"+types.RootFS+"/") {
		return strings.TrimPrefix(p, mountPrefix+"/"+types.RootFS+"/"), nil
	} else {
		return "", fmt.Errorf("Mount path %s does not start with %s/%s", p, mountPrefix, types.RootFS)
	}
}

func (f *FsMachine) mountSnap(snapId string, readonly bool) (responseEvent *types.Event, nextState StateFn) {
	fullId := f.filesystemId
	if snapId != "" {
		fullId += "@" + snapId
	}
	mountPath := mnt(fullId)
	zfsPath := fq(f.poolName, fullId)
	// only try to make the directory if it doesn't already exist
	err := os.MkdirAll(mountPath, 0775)
	if err != nil {
		log.Printf("[mount:%s] %v while trying to mkdir mountpoint %s", fullId, err, zfsPath)
		return &types.Event{
			Name: "failed-mkdir-mountpoint",
			Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	// only try to use mount.zfs if it's not already present in the output
	// of calling "mount"
	mounted, err := isFilesystemMounted(fullId)
	if err != nil {
		return &types.Event{
			Name: "failed-checking-if-mounted",
			Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	if !mounted {
		options := "noatime"
		if readonly {
			options += ",ro"
		}
		if snapId == "" {
			logZFSCommand(fullId, fmt.Sprintf("%s set canmount=noauto %s", f.zfsPath, zfsPath))
			out, err := exec.Command(f.zfsPath, "set", "canmount=noauto", zfsPath).CombinedOutput()
			if err != nil {
				return &types.Event{
					Name: "failed-settings-canmount-noauto",
					Args: &types.EventArgs{"err": err, "out": out, "zfsPath": zfsPath},
				}, backoffState
			}
		}
		logZFSCommand(fullId, fmt.Sprintf("%s -o %s %s %s", f.mountZFS, options, zfsPath, mountPath))
		out, err := exec.Command(f.mountZFS, "-o", options,
			zfsPath, mountPath).CombinedOutput()
		if err != nil {
			log.Printf("[mount:%s] %v while trying to mount %s", fullId, err, zfsPath)
			if strings.Contains(string(out), "already mounted") {
				// This can happen when the filesystem is mounted in some other
				// namespace for some reason. Try searching for it in all
				// processes' mount namespaces, and recursively unmounting it
				// from one namespace at a time until becomes free...
				firstPidNSToUnmount, rerr := func() (string, error) {
					mountTables, err := filepath.Glob("/proc/*/mounts")
					if err != nil {
						return "", err
					}
					if mountTables == nil {
						return "", fmt.Errorf("no mount tables in /proc/*/mounts")
					}
					for _, mountTable := range mountTables {
						mounts, err := ioutil.ReadFile(mountTable)
						if err != nil {
							// pids can disappear between globbing and reading
							log.Printf(
								"[mount:%s] ignoring error reading pid mount table %v: %v",
								fullId,
								mountTable, err,
							)
							continue
						}
						// return the first namespace found, as we'll unmount
						// in there and then try again (recursively)
						for _, line := range strings.Split(string(mounts), "\n") {
							if strings.Contains(line, fullId) {
								shrapnel := strings.Split(mountTable, "/")
								// e.g. (0)/(1)proc/(2)X/(3)mounts
								return shrapnel[2], nil
							}
						}
					}
					return "", fmt.Errorf("unable to find %s in any /proc/*/mounts", fullId)
				}()
				if rerr != nil {
					return &types.Event{
						Name: "failed-finding-namespace-to-unmount",
						Args: &types.EventArgs{
							"original-err": err, "original-combined-output": string(out),
							"recovery-err": rerr,
						},
					}, backoffState
				}
				log.Printf(
					"[mount:%s] attempting recovery-unmount in ns %s after %v/%v",
					fullId, firstPidNSToUnmount, err, string(out),
				)
				logZFSCommand(fullId, fmt.Sprintf("nsenter -t %s -m -u -n -i umount %s", firstPidNSToUnmount, mountPath))
				rout, rerr := exec.Command(
					"nsenter", "-t", firstPidNSToUnmount, "-m", "-u", "-n", "-i",
					"umount", mountPath,
				).CombinedOutput()
				if rerr != nil {
					return &types.Event{
						Name: "failed-recovery-unmount",
						Args: &types.EventArgs{
							"original-err": err, "original-combined-output": string(out),
							"recovery-err": rerr, "recovery-combined-output": string(rout),
						},
					}, backoffState
				}
				// recurse, maybe we've made enough progress to be able to
				// mount this time?
				//
				// TODO limit recursion depth
				return f.mountSnap(snapId, readonly)
			}
			// if there is an error - it means we could not mount so don't
			// update the filesystem with mounted = true
			return &types.Event{
				Name: "failed-mount",
				Args: &types.EventArgs{"err": err, "combined-output": string(out)},
			}, backoffState
		}
	}

	// trust that zero exit codes from mkdir && mount.zfs means
	// that it worked and that the filesystem now exists and is
	// mounted
	return &types.Event{Name: "mounted", Args: &types.EventArgs{"mount-path": mountPath}}, activeState
}

func (f *FsMachine) mount() (responseEvent *types.Event, nextState StateFn) {
	response, nextState := f.mountSnap("", false)
	if response.Name == "mounted" {
		f.filesystem.Exists = true // needed in create case
		f.filesystem.Mounted = true
	}
	return response, nextState
}

func (f *FsMachine) unmount() (responseEvent *types.Event, nextState StateFn) {
	event, nextState := f.unmountSnap("")
	if event.Name == "unmounted" {
		f.filesystem.Mounted = false
	}
	return event, nextState
}

func (f *FsMachine) unmountSnap(snapId string) (responseEvent *types.Event, nextState StateFn) {
	fsPoint := f.filesystemId
	if snapId != "" {
		fsPoint += "@" + snapId
	}
	mounted, err := isFilesystemMounted(fsPoint)
	if err != nil {
		return &types.Event{
			Name: "failed-checking-if-mounted",
			Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	if mounted {
		logZFSCommand(fsPoint, fmt.Sprintf("umount %s", mnt(fsPoint)))
		out, err := exec.Command("umount", mnt(fsPoint)).CombinedOutput()
		if err != nil {
			log.Printf("%v while trying to unmount %s", err, fq(f.poolName, fsPoint))
			return &types.Event{
				Name: "failed-unmount",
				Args: &types.EventArgs{"err": err, "combined-output": string(out)},
			}, backoffState
		}
		mounted, err := isFilesystemMounted(fsPoint)
		if err != nil {
			return &types.Event{
				Name: "failed-checking-if-mounted",
				Args: &types.EventArgs{"err": err},
			}, backoffState
		}
		if mounted {
			return f.unmountSnap(snapId)
		}
	}
	return &types.Event{Name: "unmounted"}, inactiveState
}

func isFilesystemMounted(fs string) (bool, error) {
	code, err := returnCode("mountpoint", mnt(fs))
	if err != nil {
		return false, err
	}
	return code == 0, nil
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
