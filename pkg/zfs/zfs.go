package zfs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/base64"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/metrics"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// this should be a coverall interface for the usage of zfs.
// TODO refactor usage here so that there's less duplication

type ZFS interface {
	GetPoolID() string
	GetZPoolCapacity() (float64, error)
	ReportZpoolCapacity() error
	FindFilesystemIdsOnSystem() []string
	DeleteFilesystemInZFS(fs string) error
	// Return:
	// 1. How many bytes changed since the given filesystem snapshot.
	// 2. The total number of bytes used by the filesystem. Specifically the
	//    "referenced" attribute, which means if two filesystems share some data,
	//    it'll be double-counted. Which is fine, probably, ZFS being smart is an
	//    implementation detail.
	GetDirtyDelta(filesystemId, latestSnap string) (dirtyBytes int64, usedBytes int64, err error)
	Snapshot(filesystemId, snapshotId string, meta []string) ([]byte, error)
	List(filesystemId, snapshotId string) ([]byte, error)
	FQ(filesystemId string) string
	DiscoverSystem(fs string) (*types.Filesystem, error)
	StashBranch(existingFs string, newFs string, rollbackTo string) error
	PredictSize(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string) (int64, error)
	Clone(filesystemId, originSnapshotId, newCloneFilesystemId string) ([]byte, error)
	Rollback(filesystemId, snapshotId string) ([]byte, error)
	Create(filesystemId string) ([]byte, error)
	Recv(pipeReader *io.PipeReader, toFilesystemId string, errBuffer *bytes.Buffer) error
	ApplyPrelude(prelude types.Prelude, fs string) error
	Send(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string, preludeEncoded []byte) (*io.PipeReader, chan error)
	SetCanmount(filesystemId, snapshotId string) ([]byte, error)
	Mount(filesystemId, snapshotId string, options string, mountPath string) ([]byte, error)
	Fork(filesystemId, latestSnapshot, forkFilesystemId string) error
	Diff(filesystemId string) ([]types.ZFSFileDiff, error)
	// LastModified returns last modified temp snapshot, must be called after Diff
	LastModified(filesystemID string) (*types.LastModified, error)
	DestroyTmpSnapIfExists(filesystemId string) error
}

var _ ZFS = &zfs{}

const dotmeshDiffSnapshotName = "dotmesh-fastdiff"

type zfs struct {
	zfsPath   string
	zpoolPath string
	// poolName must be set through POOL environment variable
	poolName string
	// not sure if this is needed
	mountZFS string
	poolId   string
	diffMu   sync.Mutex
}

func NewZFS(zfsPath, zpoolPath, poolName, mountZFS string) (ZFS, error) {
	zfsInter := &zfs{
		zfsPath:   zfsPath,
		zpoolPath: zpoolPath,
		poolName:  poolName,
		mountZFS:  mountZFS,
	}
	poolId, err := zfsInter.findLocalPoolId()
	if err != nil {
		utils.Out("Unable to determine pool ID. Make sure to run me as root.\n" +
			"Please create a ZFS pool called '" + poolName + "'.\n" +
			"The following commands will create a toy pool-in-a-file:\n\n" +
			"    sudo truncate -s 10G /pool-datafile\n" +
			"    sudo zpool create pool /pool-datafile\n\n" +
			"Otherwise, see 'man zpool' for how to create a real pool.\n" +
			"If you don't have the 'zpool' tool installed, on Ubuntu 16.04, run:\n\n" +
			"    sudo apt-get install zfsutils-linux\n\n" +
			"On other distributions, follow the instructions at http://zfsonlinux.org/\n")
		log.Fatalf("Unable to find pool ID, I don't know who I am :( %s %s", err, poolId)
		return nil, err
	}
	matched, err := readAndMatchPoolId(poolId)
	if err != nil {
		log.Errorf("Error matching current to old pool id, quitting")
		return nil, err
	} else if !matched {
		return nil, fmt.Errorf("The pool id for this dotmesh changed. If this is deliberate, delete the file `/dotmesh-pool-id`, otherwise investigate the issue.")
	}
	zfsInter.poolId = poolId
	return zfsInter, nil
}

func readAndMatchPoolId(currentPoolId string) (bool, error) {
	contents, err := ioutil.ReadFile("/dotmesh-pool-id")
	if err != nil {
		if os.IsNotExist(err) {
			log.Infof("Did not find old /dotmesh-pool-id file, will create it and write node id %s to it", currentPoolId)
			ioutil.WriteFile("/dotmesh-pool-id", []byte(currentPoolId), 0666)
			return true, nil
		} else {
			return false, err
		}
	}
	if string(contents) == currentPoolId {
		return true, nil
	}
	return false, nil
}

func (z *zfs) GetPoolID() string {
	return z.poolId
}

func (z *zfs) FQ(filesystemId string) string {
	// todo this is probably too much indirection, shift FQ into here when it's no longer used anywhere
	return filepath.Join(z.poolName, types.RootFS, filesystemId)
}

func (z *zfs) fullZFSFilesystemPath(filesystemId, snapshotId string) string {
	fqFilesystemId := z.FQ(FullIdWithSnapshot(filesystemId, snapshotId))
	return fqFilesystemId
}

func (z *zfs) Create(filesystemId string) ([]byte, error) {
	return z.runOnFilesystem(filesystemId, "", []string{"create"})
}

func (z *zfs) Rollback(filesystemId, snapshotId string) ([]byte, error) {

	err := z.clearMounts(filesystemId)
	if err != nil {
		return nil, err
	}

	return z.runOnFilesystem(filesystemId, snapshotId, []string{"rollback", "-Rfr"})
}

func (z *zfs) SetCanmount(filesystemId, snapshotId string) ([]byte, error) {
	return z.runOnFilesystem(filesystemId, snapshotId, []string{"set", "canmount=noauto"})
}

func (z *zfs) Mount(filesystemId, snapshotId, options, mountPath string) ([]byte, error) {
	fullFilesystemId := FullIdWithSnapshot(filesystemId, snapshotId)
	zfsFullId := z.fullZFSFilesystemPath(filesystemId, snapshotId)
	// TODO less redirection here too?
	err := os.MkdirAll(mountPath, 0775)
	if err != nil {
		log.Printf("[Mount:%s] %v while trying to create dir %s", fullFilesystemId, err, mountPath)
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fullFilesystemId,
			"mountpath":     mountPath,
		}).Error("error while trying to create a directory")
		return nil, err
	}
	LogZFSCommand(filesystemId, fmt.Sprintf("%s -o %s %s %s", z.mountZFS, options, zfsFullId, mountPath))
	output, err := exec.Command(z.mountZFS, "-o", options, zfsFullId, mountPath).CombinedOutput()
	if err != nil {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fullFilesystemId,
			"mountpath":     mountPath,
			"zfs_full_id":   zfsFullId,
			"options":       options,
			"output":        string(output),
		}).Error("error while trying to mount")
		return nil, err
	}
	return output, err
}

func (z *zfs) runOnFilesystem(filesystemId, snapshotId string, args []string) ([]byte, error) {
	fullName := z.fullZFSFilesystemPath(filesystemId, snapshotId)
	args = append(args, fullName)
	LogZFSCommand(filesystemId, fmt.Sprintf("%s %s", z.zfsPath, strings.Join(args, " ")))
	output, err := exec.Command(z.zfsPath, args...).CombinedOutput()
	if err != nil {
		log.Printf("%v while trying run command %s %s", err, z.zfsPath, strings.Join(args, " "))
	}
	return output, err
}

func (z *zfs) Snapshot(filesystemId string, snapshotId string, meta []string) ([]byte, error) {
	args := []string{"snapshot"}
	args = append(args, meta...)
	return z.runOnFilesystem(filesystemId, snapshotId, args)
}

func (z *zfs) List(filesystemId, snapshotId string) ([]byte, error) {
	return z.runOnFilesystem(filesystemId, snapshotId, []string{"list"})
}

func (z *zfs) Clone(filesystemId, originSnapshotId, newCloneFilesystemId string) ([]byte, error) {
	LogZFSCommand(filesystemId, fmt.Sprintf("%s clone %s %s", z.zfsPath, z.fullZFSFilesystemPath(filesystemId, originSnapshotId), z.FQ(newCloneFilesystemId)))
	out, err := exec.Command(
		z.zfsPath, "clone",
		z.FQ(filesystemId)+"@"+originSnapshotId,
		z.FQ(newCloneFilesystemId),
	).CombinedOutput()
	if err != nil {
		log.Printf(
			"[Clone] %v while trying to clone filesystem %s, %s -> %s",
			err, z.FQ(filesystemId), originSnapshotId, newCloneFilesystemId,
		)
	}
	return out, err
}

func (z *zfs) DiscoverSystem(fs string) (*types.Filesystem, error) {
	// TODO sanitize fs
	// does filesystem exist? (early exit if not)
	code, err := utils.ReturnCode(z.zfsPath, "list", z.FQ(fs))
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return &types.Filesystem{
			Id:     fs,
			Exists: false,
			// Important not to leave snapshots nil in the default case, we
			// need to inform other nodes that we have no snapshots of a
			// filesystem if we don't have the filesystem.
			Snapshots: []*types.Snapshot{},
		}, nil
	}
	// is filesystem mounted?

	mounted, err := utils.IsFilesystemMounted(fs)
	if err != nil {
		return nil, err
	}

	// what metadata is encoded in any snapshots' zfs properties?
	// construct metadata where it exists
	//filesystemMeta := metadata{} // TODO fs-specific metadata
	snapshotMeta := make(map[string]map[string]string)
	output, err := exec.Command(
		z.zfsPath, "get", "all", "-H", "-r", "-s", "local,received", z.FQ(fs),
	).Output()
	if err != nil {
		return nil, err
	}
	metaLines := strings.Split(string(output), "\n")
	// strip off trailing newline
	metaLines = metaLines[:len(metaLines)-1]
	for _, values := range metaLines {
		shrapnel := strings.Split(values, "\t")
		if len(shrapnel) > 3 {
			fsSnapshot := shrapnel[0]
			// strip off meta prefix
			keyEncoded := shrapnel[1]
			if strings.HasPrefix(keyEncoded, types.MetaKeyPrefix) {
				keyEncoded = keyEncoded[len(types.MetaKeyPrefix):]
				// base64 decode or die
				valueEncoded := shrapnel[2]
				var decoded []byte
				var err error
				if valueEncoded == "." {
					// special case to denote empty string
					decoded = []byte("")
				} else {
					decoded, err = base64.StdEncoding.DecodeString(valueEncoded)
					if err != nil {
						log.Printf(
							"Unable to base64 decode metadata value '%s' for %s",
							valueEncoded,
							fsSnapshot,
						)
						continue
					}
				}
				if strings.Contains(fsSnapshot, "@") {
					id := strings.Split(fsSnapshot, "@")[1]
					_, ok := snapshotMeta[id]
					if !ok {
						snapshotMeta[id] = make(map[string]string)
					}
					snapshotMeta[id][keyEncoded] = string(decoded)
				} else {
					// TODO populate filesystemMeta
				}
			}
		}
	}

	// what snapshots exist of the filesystem?
	output, err = exec.Command(z.zfsPath,
		"list", "-H", "-t", "filesystem,snapshot", "-r", z.FQ(fs)).Output()
	if err != nil {
		return nil, err
	}
	listLines := strings.Split(string(output), "\n")

	// strip off trailing newline and root pool
	listLines = listLines[1 : len(listLines)-1]
	snapshots := []*types.Snapshot{}
	for _, values := range listLines {
		fsSnapshot := strings.Split(values, "\t")[0]
		id := strings.Split(fsSnapshot, "@")[1]
		if strings.HasPrefix(id, dotmeshDiffSnapshotName) {
			continue
		}
		meta, ok := snapshotMeta[id]
		if !ok {
			meta = make(map[string]string)
		}
		snapshot := &types.Snapshot{Id: id, Metadata: meta}
		snapshots = append(snapshots, snapshot)
	}

	return &types.Filesystem{
		Id:        fs,
		Exists:    true,
		Mounted:   mounted,
		Snapshots: snapshots,
	}, nil
}

func (z *zfs) GetZPoolCapacity() (float64, error) {
	output, err := exec.Command(z.zpoolPath,
		"list", "-H", "-o", "capacity", z.poolName).Output()
	if err != nil {
		log.Fatalf("%s, when running zpool list", err)
		return 0, err
	}

	parsedCapacity := strings.Trim(string(output), "% \n")
	capacityF, err := strconv.ParseFloat(parsedCapacity, 64)
	if err != nil {
		return 0, err
	}

	return capacityF, err
}

func (z *zfs) findLocalPoolId() (string, error) {
	output, err := exec.Command(z.zfsPath, "get", "-H", "guid", z.poolName).CombinedOutput()
	if err != nil {
		return string(output), err
	}
	i, err := strconv.ParseUint(strings.Split(string(output), "\t")[2], 10, 64)
	if err != nil {
		return string(output), err
	}
	return fmt.Sprintf("%x", i), nil
}

func (z *zfs) ReportZpoolCapacity() error {
	capacity, err := z.GetZPoolCapacity()
	if err != nil {
		return err
	}
	metrics.ZPoolCapacity.WithLabelValues(z.poolId, z.poolName).Set(capacity)
	return nil
}

func (z *zfs) FindFilesystemIdsOnSystem() []string {
	// synchronously, return slice of filesystem ids that exist.
	log.Print("Finding filesystem ids...")
	listArgs := []string{"list", "-H", "-r", "-o", "name", z.poolName + "/" + types.RootFS}
	// look before you leap (check error code of zfs list)
	code, err := utils.ReturnCode(z.zfsPath, listArgs...)
	if err != nil {
		log.Fatalf("%s, when running zfs list", err)
	}
	// creates pool/dmfs on demand if it doesn't exist.
	if code != 0 {
		output, err := exec.Command(
			z.zfsPath, "create", "-o", "mountpoint=legacy", z.poolName+"/"+types.RootFS).CombinedOutput()
		if err != nil {
			utils.Out("Unable to create", z.poolName+"/"+types.RootFS, "- does ZFS pool '"+z.poolName+"' exist?\n")
			log.Printf(string(output))
			log.Fatal(err)
		}
	}
	// get output
	output, err := exec.Command(z.zfsPath, listArgs...).Output()
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
		newLines = append(newLines, UnFQ(z.poolName, line))
	}
	return newLines
}

func (z *zfs) DeleteFilesystemInZFS(fs string) error {
	LogZFSCommand(fs, fmt.Sprintf("%s destroy -r %s", z.zfsPath, FQ(z.poolName, fs)))
	cmd := exec.Command(z.zfsPath, "destroy", "-r", FQ(z.poolName, fs))
	// is there much difference between this and how runOnFilesystem works?
	err := doSimpleZFSCommand(cmd, fmt.Sprintf("delete filesystem %s (full name: %s)", fs, FQ(z.poolName, fs)))
	return err
}

func (z *zfs) GetDirtyDelta(filesystemId, latestSnap string) (int64, int64, error) {
	// Use "referenced" as the size of the filesystem, use
	// "written@<snapshotname>" for bytes written since that snapshot. See
	// https://zfsonlinux.org/manpages/0.8.1/man8/zfs.8.html
	o, err := exec.Command(
		z.zfsPath, "get", "-pH", "referenced,written@"+latestSnap, FQ(z.poolName, filesystemId),
	).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf(
			"[pollDirty] 'zfs get -pH referenced,written@%s %s' errored with: %s %s",
			FQ(z.poolName, filesystemId), latestSnap, err, o,
		)
	}

	/* Output we parse should now look this:

	   poolname/dmfs/fsname  referenced       25088   -
	   poolname/dmfs/fsname  written@myfirstsnapshot  12800   local
	*/
	var dirty int64
	var total int64
	lines := strings.Split(string(o), "\n")
	for _, line := range lines {
		shrap := strings.Fields(line)
		if len(shrap) >= 3 {
			if shrap[0] == FQ(z.poolName, filesystemId) {
				if shrap[1] == "referenced" {
					if shrap[2] == "-" {
						total = 0
					} else {
						total, err = strconv.ParseInt(shrap[2], 10, 64)
						if err != nil {
							return 0, 0, err
						}
					}
				} else if shrap[1] == ("written@" + latestSnap) {
					if shrap[2] == "-" {
						total = 0
					} else {
						dirty, err = strconv.ParseInt(shrap[2], 10, 64)
						if err != nil {
							return 0, 0, err
						}
					}
				}
			}
		}
	}

	return dirty, total, nil
}

func (z *zfs) StashBranch(existingFs string, newFs string, rollbackTo string) error {
	log.WithFields(log.Fields{
		"existing_fs": existingFs,
		"new_fs":      newFs,
		"rollback_to": rollbackTo,
	}).Info("stashing branch")
	err := z.clearMounts(existingFs)
	if err != nil {
		return err
	}

	zfsRenameCtx, zfsRenameCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer zfsRenameCancel()

	LogZFSCommand(existingFs, fmt.Sprintf("%s rename %s %s", z.zfsPath, z.FQ(existingFs), z.FQ(newFs)))
	desc := fmt.Sprintf("rename filesystem %s (%s) to %s (%s) for retroBranch", existingFs, z.FQ(existingFs), newFs, z.FQ(newFs))
	err = zfsCommandWithRetries(zfsRenameCtx, desc, z.zfsPath, "rename", z.FQ(existingFs), z.FQ(newFs))
	if err != nil {
		return err
	}

	zfsCloneCtx, zfsCloneCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer zfsCloneCancel()

	LogZFSCommand(existingFs, fmt.Sprintf("%s clone %s@%s %s", z.zfsPath, z.FQ(newFs), rollbackTo, z.FQ(existingFs)))
	desc = fmt.Sprintf("clone snapshot %s of filesystem %s (%s) to %s (%s) for retroBranch %s",
		rollbackTo, newFs, z.FQ(newFs)+"@"+rollbackTo,
		existingFs, z.FQ(existingFs), newFs,
	)
	err = zfsCommandWithRetries(zfsCloneCtx, desc, z.zfsPath, "clone", z.FQ(newFs)+"@"+rollbackTo, z.FQ(existingFs))
	if err != nil {
		return err
	}

	zfsPromoteCtx, zfsPromoteCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer zfsPromoteCancel()

	LogZFSCommand(existingFs, fmt.Sprintf("%s promote %s", z.zfsPath, z.FQ(existingFs)))
	desc = fmt.Sprintf("promote filesystem %s (%s) for retroBranch %s", existingFs, z.FQ(existingFs), newFs)
	err = zfsCommandWithRetries(zfsPromoteCtx, desc, z.zfsPath, "promote", z.FQ(existingFs))
	return err
}

/*
		Discover total number of bytes in replication stream by asking nicely:

			luke@hostess:/foo$ sudo zfs send -nP pool/foo@now2
			full    pool/foo@now2   105050056
			size    105050056
			luke@hostess:/foo$ sudo zfs send -nP -I pool/foo@now pool/foo@now2
			incremental     now     pool/foo@now2   105044936
			size    105044936

	   -n

		   Do a dry-run ("No-op") send.  Do not generate any actual send
		   data.  This is useful in conjunction with the -v or -P flags to
		   determine what data will be sent.

	   -P

		   Print machine-parsable verbose information about the stream
		   package generated.
*/
func (z *zfs) PredictSize(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string) (int64, error) {
	sendArgs := z.calculateSendArgs(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId)
	predictArgs := []string{"send", "-nP"}
	predictArgs = append(predictArgs, sendArgs...)

	sizeCmd := exec.Command(z.zfsPath, predictArgs...)

	log.Printf("[predictSize] predict command: %#v", sizeCmd)

	out, err := sizeCmd.CombinedOutput()
	log.Printf("[predictSize] Output of predict command: %v", string(out))
	if err != nil {
		log.Printf("[predictSize] Got error on predict command: %v", err)
		return 0, err
	}
	shrap := strings.Split(string(out), "\n")
	if len(shrap) < 2 {
		return 0, fmt.Errorf("Not enough lines in output %v", string(out))
	}
	sizeLine := shrap[len(shrap)-2]
	shrap = strings.Fields(sizeLine)
	if len(shrap) < 2 {
		return 0, fmt.Errorf("Not enough fields in %v", sizeLine)
	}

	size, err := strconv.ParseInt(shrap[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func (z *zfs) calculateSendArgs(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string) []string {

	// toFilesystemId
	// snapRange.toSnap.Id
	// snapRange.fromSnap == nil?  --> fromSnapshotId == ""?
	// snapRange.fromSnap.Id

	var sendArgs []string
	var fromSnap string
	if fromSnapshotId == "" {
		fromSnap = "START"
		if fromFilesystemId != "" { // XXX wtf
			// This is a clone-origin based send
			fromSnap = fmt.Sprintf(
				"%s@%s", fromFilesystemId, fromSnapshotId,
			)
		}
	} else {
		fromSnap = fromSnapshotId
	}
	if fromSnap == "START" {
		// -R sends interim snapshots as well
		sendArgs = []string{
			"-p", "-R", z.FQ(toFilesystemId) + "@" + toSnapshotId,
		}
	} else {
		// in clone case, fromSnap must be fully qualified
		if strings.Contains(fromSnap, "@") {
			// send a clone, so make it fully qualified
			fromSnap = z.FQ(fromSnap)
		}
		sendArgs = []string{
			"-p", "-I", fromSnap, z.FQ(toFilesystemId) + "@" + toSnapshotId,
		}
	}
	return sendArgs
}

func (z *zfs) Recv(pipeReader *io.PipeReader, toFilesystemId string, errBuffer *bytes.Buffer) error {
	cmd := exec.Command(z.zfsPath, "recv", z.FQ(toFilesystemId))

	cmd.Stdin = pipeReader
	cmd.Stdout = utils.GetLogfile("zfs-recv-stdout")
	if errBuffer == nil {
		cmd.Stderr = utils.GetLogfile("zfs-recv-stderr")
	} else {
		cmd.Stderr = errBuffer
	}
	return cmd.Run()
}

func (z *zfs) ApplyPrelude(prelude types.Prelude, fs string) error {
	// iterate over it setting zfs user properties accordingly.
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
				args = append(args, z.FQ(fs)+"@"+j.Id)
				out, err := exec.Command(z.zfsPath, args...).CombinedOutput()
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

func (z *zfs) Send(fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string, preludeEncoded []byte) (*io.PipeReader, chan error) {
	log.WithFields(log.Fields{
		"fromFilesystemId": fromFilesystemId,
		"fromSnapshotId":   fromSnapshotId,
		"toFilesystemId":   toFilesystemId,
		"toSnapshotId":     toSnapshotId,
	}).Debug("zfs.Send() starting")
	sendArgs := z.calculateSendArgs(
		fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId,
	)
	realArgs := []string{"send"}
	realArgs = append(realArgs, sendArgs...)
	LogZFSCommand(fromFilesystemId, fmt.Sprintf("%s %s", z.zfsPath, strings.Join(realArgs, " ")))
	cmd := exec.Command(z.zfsPath, realArgs...)
	pipeReader, pipeWriter := io.Pipe()
	cmd.Stdout = pipeWriter
	cmd.Stderr = utils.GetLogfile("zfs-send-errors")
	errch := make(chan error)
	go func() {
		// This goroutine does all the writing to the HTTP POST
		// log.Printf(
		// 	"[actualPush:%s] Writing prelude of %d bytes (encoded): %s",
		// 	filesystemId,
		// 	len(preludeEncoded), preludeEncoded,
		// )
		bytes, err := pipeWriter.Write(preludeEncoded)
		if err != nil {
			log.Errorf("[actualPush:%s] Error writing prelude: %+v, %d bytes sent (sent to errch)", fromFilesystemId, err, bytes)
			errch <- err
			log.Errorf("[actualPush:%s] errch accepted prelude error, woohoo", fromFilesystemId)
		}

		log.Infof(
			"[actualPush:%s] About to Run() for %s => %s",
			fromFilesystemId, fromSnapshotId, toSnapshotId,
		)

		runErr := cmd.Run()

		log.Debugf(
			"[actualPush:%s] Run() got result %s, about to put it into errch after closing pipeWriter",
			fromFilesystemId,
			runErr,
		)
		err = pipeWriter.Close()
		if err != nil {
			log.Errorf("[actualPush:%s] error closing pipeWriter: %s", fromFilesystemId, err)
		}
		log.Debugf(
			"[actualPush:%s] Writing to errch: %+v",
			fromFilesystemId,
			runErr,
		)
		errch <- runErr
		log.Infof("[actualPush:%s] errch accepted it, woohoo", fromFilesystemId)
		pipeWriter.Close()
	}()
	return pipeReader, errch
}

func (z *zfs) Fork(filesystemId, latestSnapshot, forkFilesystemId string) error {
	sendCommand := exec.Command(z.zfsPath, "send", "-R", z.fullZFSFilesystemPath(filesystemId, latestSnapshot))
	recvCommand := exec.Command(z.zfsPath, "recv", z.fullZFSFilesystemPath(forkFilesystemId, ""))
	in, out, err := os.Pipe()
	if err != nil {
		return err
	}
	recvCommand.Stdin = in
	sendCommand.Stdout = out

	sendResultChan := make(chan error)
	defer close(sendResultChan)

	start := time.Now()

	go func() {
		err := sendCommand.Run()

		if err != nil {
			log.WithError(err).WithField("command", sendCommand).Error("Error running zfs send command")
		}

		sendResultChan <- err
	}()

	result, err := recvCommand.CombinedOutput()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"command": sendCommand,
			"output":  string(result),
		}).Error("Error running zfs receive command")
		return err
	}

	err = <-sendResultChan
	if err != nil {
		return err
	}

	log.WithField("duration", fmt.Sprintf("%v", time.Since(start))).Info("ZFS fork completed")

	return nil
}

type DiffResult struct {
	mtime string
	size  string
}
type DiffSide map[string]DiffResult

func diffSideFromLines(result []byte) (DiffSide, error) {
	lines := strings.Split(string(result), "\n")
	ds := DiffSide{}
	for _, line := range lines {
		if line == "" {
			continue
		}
		shrapnel := strings.SplitN(line, " ", 3)
		if len(shrapnel) < 3 {
			return nil, fmt.Errorf("too few parts")
		}
		mtime := shrapnel[0]
		size := shrapnel[1]
		filename := shrapnel[2]
		prefix := "./__default__/"
		if !strings.HasPrefix(filename, prefix) {
			continue
		}
		filename = filename[len(prefix):]
		ds[filename] = DiffResult{mtime: mtime, size: size}
	}
	return ds, nil
}

// NB: the following caches would be better on an object than as globals.

type FilesystemDiffCache struct {
	// latest snapshot cached (we only cache one DiffSide result per
	// filesystem, this points to which snapshot it is for)
	SnapshotID string
	DiffSide   DiffSide
}

type FilesystemResultCache struct {
	SnapshotID string
	Result     []types.ZFSFileDiff
}

// map from filesystem id to cached DiffSide for latest snap inspected
var diffSideCache = map[string]FilesystemDiffCache{}

// map from filesystem id to cached final result in case where tmp snap has
// zero size (no changes since last time it was run)
var diffResultCache = map[string]FilesystemResultCache{}

func (z *zfs) DestroyTmpSnapIfExists(filesystemID string) error {
	// Don't accidentally include the dotmesh-fastdiff snapshot in a push stream.
	tmp := z.FQ(FullIdWithSnapshot(filesystemID, dotmeshDiffSnapshotName))

	tmpExistsErr := exec.Command(z.zfsPath, "get", "name", tmp).Run()
	if tmpExistsErr == nil {
		return exec.Command(z.zfsPath, "destroy", tmp).Run()
	}
	return nil
}

func (z *zfs) LastModified(filesystemID string) (*types.LastModified, error) {
	// zfs list -o creation pool/dmfs/06eb69e0-c635-4ada-8539-f827f75aeeff@dotmesh-fastdiff
	// CREATION
	// Tue Oct 15 11:01 2019
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bts, err := exec.CommandContext(ctx, z.zfsPath, "list", "-p", "-o", "creation", z.FQ(FullIdWithSnapshot(filesystemID, dotmeshDiffSnapshotName))).CombinedOutput()
	if err != nil {
		return nil, err
	}

	t, err := parseSnapshotCreationTime(string(bts))
	if err != nil {
		return nil, err
	}

	return &types.LastModified{
		Time: *t,
	}, nil
}

func parseSnapshotCreationTime(commandOutput string) (*time.Time, error) {

	lines := strings.Split(string(commandOutput), "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("unexpected command output: %s", commandOutput)
	}

	timestamp, err := strconv.ParseInt(lines[1], 10, 64)
	if err != nil {
		return nil, err
	}
	t := time.Unix(timestamp, 0).UTC()
	return &t, err
}

func (z *zfs) Diff(filesystemID string) ([]types.ZFSFileDiff, error) {

	/*
		Diff the default subdot of a given dot against the latest commit on
		that dot.

		NB:
		0. the snapshot arg is ignored. (in dotscience, the committer was
		   sometimes giving us a commit id which doesn't exist on the dot --
		   possibly it existed on a fork origin.)
		1. the snapshotOrFilesystem arg is ignored.
	*/

	// NB: DiscoverSystem ignores the tmpSnapshotName
	filesystemInfo, err := z.DiscoverSystem(filesystemID)
	if err != nil {
		log.WithError(err).Error("[diff] error discover system to find latest snap")
		return nil, err
	}
	if len(filesystemInfo.Snapshots) == 0 {
		return nil, fmt.Errorf("cannot diff against a filesystem with no snapshots")
	}
	snapshot := filesystemInfo.Snapshots[len(filesystemInfo.Snapshots)-1].Id

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Minute)
	defer cancel()

	// latest is the latest "dotmesh" commit
	latest := z.fullZFSFilesystemPath(filesystemID, snapshot)

	// tmp is a new, temporary snapshot which is newer than the "latest"
	// snapshot
	tmp := z.FQ(FullIdWithSnapshot(filesystemID, dotmeshDiffSnapshotName))

	latestMnt := utils.Mnt("diff-latest-" + filesystemID)
	tmpMnt := utils.Mnt("diff-tmp-" + filesystemID)

	// First, if the dotmesh-fastdiff snapshot exists and there's no dirty data
	// on it, and we have a cached diffResultCache, return it
	cmd := exec.CommandContext(ctx, z.zfsPath, "get", "name", tmp)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	tmpExistsErr := cmd.Run()
	if tmpExistsErr == nil {
		dirty, _, err := z.GetDirtyDelta(filesystemID, tmp)
		if err != nil {
			log.WithError(err).Error("[diff] error get dirty delta")
			return nil, err
		}
		if dirty == 0 {
			// try to use the cache
			if result, ok := diffResultCache[filesystemID]; ok {
				if result.SnapshotID == snapshot {
					// Don't delete this, it's used by tests:
					log.WithFields(log.Fields{"diff_used_cache": true}).Debug("Used cache")
					return result.Result, nil
				}
			}
		}
	}
	// Don't delete this, it's used by tests:
	log.WithFields(log.Fields{"diff_used_cache": false}).Debug("Didn't use the cache")

	err = os.MkdirAll(tmpMnt, 0775)
	if err != nil {
		log.WithError(err).Error("[diff] error mkdir tmpMnt")
		return nil, err
	}

	// it's ok if these fail, they are just cleanup from previous runs if they
	// happened
	exec.CommandContext(ctx, "umount", latestMnt).Run()
	exec.CommandContext(ctx, "umount", tmpMnt).Run()
	exec.CommandContext(ctx, z.zfsPath, "destroy", tmp).Run()

	_, err = z.Snapshot(filesystemID, dotmeshDiffSnapshotName, []string{})
	if err != nil {
		log.WithError(err).Error("[diff] error snapshot")
		return nil, err
	}

	err = exec.CommandContext(ctx, "mount", "-t", "zfs", tmp, tmpMnt).Run()
	if err != nil {
		log.WithError(err).Error("[diff] error mount tmp")
		return nil, err
	}

	findCmdTmpl := `(cd %s; find . -printf "%%T+ %%s %%p\n")`

	// only mount & fetch file list from latest if we haven't got it cached already

	var mapLatest DiffSide
	var mountedLatest bool = false

	z.diffMu.Lock()
	latestCache, ok := diffSideCache[filesystemID]
	z.diffMu.Unlock()

	if !ok {
		log.WithFields(log.Fields{
			"filesystem_id": filesystemID,
		}).Info("cached snapshot not found")
	}

	// if latestCache, ok := diffSideCache[filesystemID]; ok && latestCache.SnapshotID == snapshot {
	if ok && latestCache.SnapshotID == snapshot {
		mapLatest = latestCache.DiffSide
	} else {
		log.Infof("setting up new diffSideCache for filesystem %s", filesystemID)
		// do all setup for latestMnt only in the case that we actually need it
		// (can't read it from in-memory cache)
		err := os.MkdirAll(latestMnt, 0775)
		if err != nil {
			log.WithError(err).Error("[diff] error mkdir latestMnt")
			return nil, err
		}
		out, err := exec.CommandContext(ctx, "mount", "-t", "zfs", latest, latestMnt).CombinedOutput()
		if err != nil {
			log.WithError(err).Errorf("[diff] error mount latest: %s", string(out))
			return nil, err
		}
		mountedLatest = true
		latestFiles, err := exec.CommandContext(
			ctx, "bash", "-c", fmt.Sprintf(findCmdTmpl, latestMnt)).CombinedOutput()
		if err != nil {
			log.WithError(err).Error("[diff] getting latest files")
			return nil, err
		}
		mapLatest, err = diffSideFromLines(latestFiles)
		if err != nil {
			log.WithError(err).Error("[diff] parsing latest files")
			return nil, err
		}
		z.diffMu.Lock()
		diffSideCache[filesystemID] = FilesystemDiffCache{
			SnapshotID: snapshot,
			DiffSide:   mapLatest,
		}
		z.diffMu.Unlock()
	}

	tmpFiles, err := exec.CommandContext(
		ctx, "bash", "-c", fmt.Sprintf(findCmdTmpl, tmpMnt)).CombinedOutput()
	if err != nil {
		log.WithError(err).Error("[diff] getting tmp files")
		return nil, err
	}
	mapTmp, err := diffSideFromLines(tmpFiles)
	if err != nil {
		log.WithError(err).Error("[diff] parsing tmp files")
		return nil, err
	}

	result := map[string]types.ZFSFileDiff{}
	resultFiles := []string{}

	for filename, tmpProps := range mapTmp {
		if latestProps, ok := mapLatest[filename]; ok {
			// exists in previous snap, check if modified
			if tmpProps != latestProps {
				// modified!
				resultFiles = append(resultFiles, filename)
				result[filename] = types.ZFSFileDiff{
					Change:   types.FileChangeModified,
					Filename: filename,
				}
			}
		} else {
			// does not exist in previous snap, created
			resultFiles = append(resultFiles, filename)
			result[filename] = types.ZFSFileDiff{
				Change:   types.FileChangeAdded,
				Filename: filename,
			}
		}
	}
	for filename, _ := range mapLatest {
		if _, ok := mapTmp[filename]; !ok {
			// exists in latest but not tmp, must have been deleted
			resultFiles = append(resultFiles, filename)
			result[filename] = types.ZFSFileDiff{
				Change:   types.FileChangeRemoved,
				Filename: filename,
			}
		}
	}
	sort.Strings(resultFiles)
	sortedResult := []types.ZFSFileDiff{}
	for _, file := range resultFiles {
		sortedResult = append(sortedResult, result[file])
	}

	// only try to clean up latest mount if we needed to mount it at all
	if mountedLatest {
		out, err := exec.CommandContext(ctx, "umount", latestMnt).CombinedOutput()
		if err != nil {
			log.WithError(err).Errorf("[diff] failed unmounting latest: %s", string(out))
			return nil, err
		}
		err = exec.CommandContext(ctx, "rmdir", latestMnt).Run()
		if err != nil {
			log.WithError(err).Error("[diff] failed cleaning up latest mount")
			return nil, err
		}
	}

	out, err := exec.CommandContext(ctx, "umount", tmpMnt).CombinedOutput()
	if err != nil {
		log.WithError(err).Errorf("[diff] failed unmounting tmp: %s", string(out))
		return nil, err
	}

	// NB: we don't destroy the tmp snap here because we want to compare its
	// dirty data value to know whether to use the cache next time round. It
	// will get cleaned up if there is dirty data and a new tmp snap created
	// then.

	err = exec.CommandContext(ctx, "rmdir", tmpMnt).Run()
	if err != nil {
		log.WithError(err).Error("[diff] failed cleaning up tmp mount")
		return nil, err
	}

	// stash for later
	// TODO: protect this map with a mutex
	diffResultCache[filesystemID] = FilesystemResultCache{
		SnapshotID: snapshot,
		Result:     sortedResult,
	}

	return sortedResult, nil
}

func (z *zfs) clearMounts(filesystem string) error {

	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	mountPrefix := os.Getenv("MOUNT_PREFIX")

	mountpoints, err := filterMountpoints(mountPrefix, filesystem, r)
	if err != nil {
		return fmt.Errorf("failed to get filesystem %s mountpoints, error: %s", filesystem, err)
	}

	for _, m := range mountpoints {

		ns, err := namespaceForMount(m)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"mountpoint":    m,
				"filesystem_id": filesystem,
			}).Error("clearMounts: failed to get namespace for mount")
			continue
		}

		log.WithFields(log.Fields{
			"filesystem": filesystem,
			"mountpoint": m,
			"namespace":  ns,
		}).Info("clearMounts: unmounting..")

		err = unmount(ns, m)
		if err != nil {
			log.WithFields(log.Fields{
				"error":      err,
				"filesystem": filesystem,
				"mountpoint": m,
				"namespace":  ns,
			}).Error("failed to clear mountpoint")
			return fmt.Errorf("failed to clear filesystem %s mountpoint %s, error: %s", filesystem, m, err)
		}
	}

	return nil
}

func unmount(ns, mountpoint string) error {
	out, err := exec.Command(
		"nsenter", "-t", ns, "-a",
		"umount", mountpoint,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed nsenter umount of %s in ns %s, err: %s, out: %s", mountpoint, ns, err, out)
	}
	return nil
}

func namespaceForMount(mountpoint string) (string, error) {
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
			continue
		}
		for _, line := range strings.Split(string(mounts), "\n") {
			if strings.Contains(line, mountpoint) {
				shrapnel := strings.Split(mountTable, "/")
				// e.g. (0)/(1)proc/(2)X/(3)mounts
				return shrapnel[2], nil
			}
		}
	}
	return "", nil
}
