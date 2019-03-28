package zfs

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"encoding/base64"
	"io"
	"os"

	"github.com/dotmesh-io/dotmesh/pkg/metrics"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/utils"
)

// this should be a coverall interface for the usage of zfs.
// TODO refactor usage here so that there's less duplication

type ZFS interface {
	GetPoolID() string
	GetZPoolCapacity() (float64, error)
	ReportZpoolCapacity() error
	FindFilesystemIdsOnSystem() []string
	DeleteFilesystemInZFS(fs string) error
	GetDirtyDelta(filesystemId, latestSnap string) (int64, int64, error)
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
}

type zfs struct {
	zfsPath   string
	zpoolPath string
	// poolName must be set through POOL environment variable
	poolName string
	// not sure if this is needed
	mountZFS string
	poolId   string
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
	return fmt.Sprintf("%s/%s/%s", z.poolName, types.RootFS, filesystemId)
}

func (z *zfs) fullZFSFilesystemPath(filesystemId, snapshotId string) string {
	fqFilesystemId := z.FQ(FullIdWithSnapshot(filesystemId, snapshotId))
	return fqFilesystemId
}

func (z *zfs) Create(filesystemId string) ([]byte, error) {
	return z.runOnFilesystem(filesystemId, "", []string{"create"})
}

func (z *zfs) Rollback(filesystemId, snapshotId string) ([]byte, error) {
	return z.runOnFilesystem(filesystemId, snapshotId, []string{"rollback", "-r"})
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
		return nil, err
	}
	LogZFSCommand(filesystemId, fmt.Sprintf("%s -o %s %s %s", z.mountZFS, options, zfsFullId, mountPath))
	output, err := exec.Command(z.mountZFS, "-o", options, zfsFullId, mountPath).CombinedOutput()
	if err != nil {
		log.Printf("[Mount:%s] %v while trying to mount %s", fullFilesystemId, err, zfsFullId)
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
		log.Printf("[list] %v while trying to list snapshot for filesystem %s", err, z.FQ(filesystemId))
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
	o, err := exec.Command(
		z.zfsPath, "get", "-pHr", "referenced,used", FQ(z.poolName, filesystemId),
	).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf(
			"[pollDirty] 'zfs get -pHr referenced,used %s' errored with: %s %s",
			FQ(z.poolName, filesystemId), err, o,
		)
	}
	/*
		pool/y  referenced      104948736       -
		pool/y  used    209887232       -
		pool/y@now      referenced      104948736       -
		pool/y@now      used    104938496       -
	*/
	var referDataset, referLatestSnap, usedLatestSnap, usedDataset int64
	lines := strings.Split(string(o), "\n")
	for _, line := range lines {
		shrap := strings.Fields(line)
		if len(shrap) >= 3 {
			if shrap[0] == FQ(z.poolName, filesystemId) {
				if shrap[1] == "referenced" {
					referDataset, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				} else if shrap[1] == "used" {
					usedDataset, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				}
			} else if shrap[0] == FQ(z.poolName, filesystemId)+"@"+latestSnap {
				if shrap[1] == "referenced" {
					referLatestSnap, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				} else if shrap[1] == "used" {
					usedLatestSnap, err = strconv.ParseInt(shrap[2], 10, 64)
					if err != nil {
						return 0, 0, err
					}
				}
			}
		}
	}
	// Dirty filesystems that have been rolled back to the latest snapshot
	// sometimes exhibit 1024 bytes used.
	if usedLatestSnap <= 1024 {
		usedLatestSnap = 0
	}
	return intDiff(referDataset, referLatestSnap) + usedLatestSnap, usedDataset, nil
}

func intDiff(a, b int64) int64 {
	if a-b < 0 {
		return b - a
	} else {
		return a - b
	}
}

type savedMount struct {
	Mountpoint string // Actual filesystem mountpoint
	MountedFS  string // ZFS pool location we mounted there
}

func (z *zfs) StashBranch(existingFs string, newFs string, rollbackTo string) error {
	mounts := []savedMount{}

	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	mountPrefix := os.Getenv("MOUNT_PREFIX")

	for {
		line, err := r.ReadString('\n')

		if line != "" {
			parts := strings.Split(line, " ")
			if len(parts) >= 11 {
				fsType := parts[8]
				mountpoint := parts[4]
				mountedFS := parts[9]
				if fsType == "zfs" && strings.HasPrefix(mountpoint, mountPrefix) {
					mounts = append(mounts, savedMount{
						Mountpoint: mountpoint,
						MountedFS:  mountedFS,
					})
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
	}

	log.Debugf("ABS TEST: Got mountpoints: %#v\n", mounts)

	LogZFSCommand(existingFs, fmt.Sprintf("%s rename %s %s", z.zfsPath, z.FQ(existingFs), z.FQ(newFs)))
	err = doSimpleZFSCommand(exec.Command(z.zfsPath, "rename", z.FQ(existingFs), z.FQ(newFs)),
		fmt.Sprintf("rename filesystem %s (%s) to %s (%s) for retroBranch",
			existingFs, z.FQ(existingFs),
			newFs, z.FQ(newFs),
		),
	)
	if err != nil {
		return err
	}

	LogZFSCommand(existingFs, fmt.Sprintf("%s clone %s@%s %s", z.zfsPath, z.FQ(newFs), rollbackTo, z.FQ(existingFs)))
	err = doSimpleZFSCommand(exec.Command(z.zfsPath, "clone", z.FQ(newFs)+"@"+rollbackTo, z.FQ(existingFs)),
		fmt.Sprintf("clone snapshot %s of filesystem %s (%s) to %s (%s) for retroBranch",
			rollbackTo, newFs, z.FQ(newFs)+"@"+rollbackTo,
			existingFs, z.FQ(existingFs),
		),
	)
	if err != nil {
		return err
	}

	LogZFSCommand(existingFs, fmt.Sprintf("%s promote %s", z.zfsPath, z.FQ(existingFs)))
	err = doSimpleZFSCommand(exec.Command(z.zfsPath, "promote", z.FQ(existingFs)),
		fmt.Sprintf("promote filesystem %s (%s) for retroBranch",
			existingFs, z.FQ(existingFs),
		),
	)
	if err != nil {
		return err
	}

	return nil
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
	log.Infof("[applyPrelude] Got prelude: %+v", prelude)
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
		"prelude":          string(preludeEncoded),
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
			"output":  result,
		}).Error("Error running zfs receive command")
		return err
	}

	err = <-sendResultChan
	if err != nil {
		return err
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.WithField("duration", fmt.Sprintf("%v", elapsed)).Info("ZFS fork completed")

	return nil
}
