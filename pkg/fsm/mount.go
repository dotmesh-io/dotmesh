package fsm

import (
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
	"github.com/dotmesh-io/dotmesh/pkg/zfs"
)

// TODO: remove that environment getter and replace with parameter

func (f *FsMachine) mountSnap(snapId string, readonly bool) (responseEvent *types.Event, nextState StateFn) {
	// only try to use mount.zfs if it's not already present in the output
	// of calling "mount"
	fullId := zfs.FullIdWithSnapshot(f.filesystemId, snapId)
	mounted, err := utils.IsFilesystemMounted(fullId)
	mountPath := utils.Mnt(fullId)
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
			out, err := f.zfs.SetCanmount(f.filesystemId, snapId)
			if err != nil {
				return &types.Event{
					Name: "failed-settings-canmount-noauto",
					Args: &types.EventArgs{"err": err, "out": out, "zfsPath": f.zfs.FQ(fullId)},
				}, backoffState
			}
		}
		out, err := f.zfs.Mount(f.filesystemId, snapId, options, mountPath)
		if err != nil {
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
				// this is a misnomer as it's not actually a zfs command...
				zfs.LogZFSCommand(fullId, fmt.Sprintf("nsenter -t %s -m -u -n -i umount %s", firstPidNSToUnmount, mountPath))
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
	mounted, err := utils.IsFilesystemMounted(fsPoint)
	if err != nil {
		return &types.Event{
			Name: "failed-checking-if-mounted",
			Args: &types.EventArgs{"err": err},
		}, backoffState
	}
	if mounted {
		zfs.LogZFSCommand(fsPoint, fmt.Sprintf("umount %s", utils.Mnt(fsPoint)))
		out, err := exec.Command("umount", utils.Mnt(fsPoint)).CombinedOutput()
		if err != nil {
			log.Printf("%v while trying to unmount %s", err, f.zfs.FQ(fsPoint))
			return &types.Event{
				Name: "failed-unmount",
				Args: &types.EventArgs{"err": err, "combined-output": string(out)},
			}, backoffState
		}
		mounted, err := utils.IsFilesystemMounted(fsPoint)
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
