package fsm

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/dotmesh-io/dotmesh/pkg/types"
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

// how many bytes has a filesystem diverged from its latest snapshot?
// also how many bytes does the filesystem take up on disk in total?
// TODO rename getDirtyDelta and dirtyInfo etc to sizeInfo
func getDirtyDelta(zfsExec, poolName, filesystemId, latestSnap string) (int64, int64, error) {
	o, err := exec.Command(
		zfsExec, "get", "-pHr", "referenced,used", fq(poolName, filesystemId),
	).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf(
			"[pollDirty] 'zfs get -pHr referenced,used %s' errored with: %s %s",
			fq(poolName, filesystemId), err, o,
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
			if shrap[0] == fq(poolName, filesystemId) {
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
			} else if shrap[0] == fq(poolName, filesystemId)+"@"+latestSnap {
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

func findLocalPoolId(zpoolExec, pool string) (string, error) {
	output, err := exec.Command(zpoolExec, "get", "-H", "guid", pool).CombinedOutput()
	if err != nil {
		return string(output), err
	}
	i, err := strconv.ParseUint(strings.Split(string(output), "\t")[2], 10, 64)
	if err != nil {
		return string(output), err
	}
	return fmt.Sprintf("%x", i), nil
}

func getZpoolCapacity(zpoolExec, pool string) (float64, error) {
	output, err := exec.Command(zpoolExec,
		"list", "-H", "-o", "capacity", pool).Output()
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

func findFilesystemIdsOnSystem(zfsExec, pool string) []string {
	// synchronously, return slice of filesystem ids that exist.
	log.Print("Finding filesystem ids...")
	listArgs := []string{"list", "-H", "-r", "-o", "name", pool + "/" + types.RootFS}
	// look before you leap (check error code of zfs list)
	code, err := returnCode(zfsExec, listArgs...)
	if err != nil {
		log.Fatalf("%s, when running zfs list", err)
	}
	// creates pool/dmfs on demand if it doesn't exist.
	if code != 0 {
		output, err := exec.Command(
			zfsExec, "create", "-o", "mountpoint=legacy", pool+"/"+types.RootFS).CombinedOutput()
		if err != nil {
			out("Unable to create", pool+"/"+types.RootFS, "- does ZFS pool '"+pool+"' exist?\n")
			log.Printf(string(output))
			log.Fatal(err)
		}
	}
	// get output
	output, err := exec.Command(zfsExec, listArgs...).Output()
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
		newLines = append(newLines, unfq(pool, line))
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

func deleteFilesystemInZFS(zfsExec, poolName, fs string) error {
	logZFSCommand(fs, fmt.Sprintf("%s destroy -r %s", zfsExec, fq(poolName, fs)))
	cmd := exec.Command(zfsExec, "destroy", "-r", fq(poolName, fs))
	err := doSimpleZFSCommand(cmd, fmt.Sprintf("delete filesystem %s (full name: %s)", fs, fq(poolName, fs)))
	return err
}

// Make a new clone newFS of existingFS, and then roll existingFS back
// to snapshot rollbackTo.  This is easier said than done, as ZFS hs
// rules about rolling the parent back past a clone point, but we have
// ways around that.

// We have (branch is called foo, rollbackTo = C)

// us:     A -> B -> C -> D(foo)

// We must bring about this situation, with a new automatically made
// "foo-oops" branch to preserve the rolled-back-past state:

// us:     A -> B -> C (foo)
//                   \ -> D(foo-oops)

// HOW NOT TO DO IT

// step 1: create foo-oops by cloning foo
// us:     A -> B -> C -> D(foo)(foo-oops)

// step 2: move foo back to C, the rollbackTo point
// us:     A -> B -> C(foo)                - !!! ZFS DISALLOWS THIS !!!
//                   \ -> D(foo-oops)

// CAN WE DO IT WITH PROMOTION?

// It seems that if we create foo-oops by cloning foo, and then
// promote foo-oops, all the snapshots become snapshots of foo-oops
// rather than foo, and so foo@C doesn't exist, and foo-oops can't be
// rolled back to foo-oops@C because of foo being a child of
// it.. Hmmm.

// CAN WE DO IT WITH RENAMES? YES, WE CAN!

// step 1: rename foo to foo-oops
// us:     A -> B -> C -> D(foo-oops)
// # zfs rename foo foo-oops

// step 2: create foo from foo-oops@C:
// us:     A -> B -> C -> D(foo-oops)
//                   \(foo)
// # zfs clone foo-oops@C foo

// step 3: make foo-oops be the branch from foo
// us:     A -> B -> C (foo)
//                   \ -> D(foo-oops)
// # zfs promote foo

func stashBranch(zfsExec, poolName string, existingFs string, newFs string, rollbackTo string) error {
	logZFSCommand(existingFs, fmt.Sprintf("%s rename %s %s", zfsExec, fq(poolName, existingFs), fq(poolName, newFs)))
	err := doSimpleZFSCommand(exec.Command(zfsExec, "rename", fq(poolName, existingFs), fq(poolName, newFs)),
		fmt.Sprintf("rename filesystem %s (%s) to %s (%s) for retroBranch",
			existingFs, fq(poolName, existingFs),
			newFs, fq(poolName, newFs),
		),
	)
	if err != nil {
		return err
	}

	logZFSCommand(existingFs, fmt.Sprintf("%s clone %s@%s %s", zfsExec, fq(poolName, newFs), rollbackTo, fq(poolName, existingFs)))
	err = doSimpleZFSCommand(exec.Command(zfsExec, "clone", fq(poolName, newFs)+"@"+rollbackTo, fq(poolName, existingFs)),
		fmt.Sprintf("clone snapshot %s of filesystem %s (%s) to %s (%s) for retroBranch",
			rollbackTo, newFs, fq(poolName, newFs)+"@"+rollbackTo,
			existingFs, fq(poolName, existingFs),
		),
	)
	if err != nil {
		return err
	}

	logZFSCommand(existingFs, fmt.Sprintf("%s promote %s", zfsExec, fq(poolName, existingFs)))
	err = doSimpleZFSCommand(exec.Command(zfsExec, "promote", fq(poolName, existingFs)),
		fmt.Sprintf("promote filesystem %s (%s) for retroBranch",
			existingFs, fq(poolName, existingFs),
		),
	)
	if err != nil {
		return err
	}

	return nil
}

func discoverSystem(zfsExec, poolName, fs string) (*types.Filesystem, error) {
	// TODO sanitize fs
	// does filesystem exist? (early exit if not)
	code, err := returnCode(zfsExec, "list", fq(poolName, fs))
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

	mounted, err := isFilesystemMounted(fs)
	if err != nil {
		return nil, err
	}

	// what metadata is encoded in any snapshots' zfs properties?
	// construct metadata where it exists
	//filesystemMeta := metadata{} // TODO fs-specific metadata
	snapshotMeta := map[string]types.Metadata{}
	output, err := exec.Command(
		zfsExec, "get", "all", "-H", "-r", "-s", "local,received", fq(poolName, fs),
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

				decoded, err := base64.StdEncoding.DecodeString(valueEncoded)
				if err != nil {
					log.Printf(
						"Unable to base64 decode metadata value '%s' for %s",
						valueEncoded,
						fsSnapshot,
					)
				} else {
					if strings.Contains(fsSnapshot, "@") {
						id := strings.Split(fsSnapshot, "@")[1]
						_, ok := snapshotMeta[id]
						if !ok {
							snapshotMeta[id] = types.Metadata{}
						}
						snapshotMeta[id][keyEncoded] = string(decoded)
					} else {
						// TODO populate filesystemMeta
					}
				}
			}
		}
	}

	// what snapshots exist of the filesystem?
	output, err = exec.Command(zfsExec,
		"list", "-H", "-t", "filesystem,snapshot", "-r", fq(poolName, fs)).Output()
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
			meta = make(types.Metadata)
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
